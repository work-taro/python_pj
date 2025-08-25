import re
import dbf
import sys
import time
import pandas as pd
import datetime as dt
from threading import Thread
from sqlalchemy import create_engine, text

# --- Config ---
dbf_file_aptrn = "C:/Users/kc/Desktop/DBF/APTRN.DBF"
dbf_file_artrn = "C:/Users/kc/Desktop/DBF/ARTRN.DBF"
dbf_file_bktrn = "C:/Users/kc/Desktop/DBF/BKTRN.DBF"
dbf_file_bkmas = "C:/Users/kc/Desktop/DBF/BKMAS.DBF"

# Database config
db_host = 'localhost'
db_database = 'kacee_center'
db_username = 'root'
db_password = ''
db_port = 3306
db_url = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4"
engine = create_engine(db_url)

start_date = dt.date(2025, 7, 1)
# end_date = pd.Timestamp.today()
# start_date = (end_date - pd.DateOffset(months=2)).replace(day=1)

# -----------------------------
# Utils: fast datetime & date
# -----------------------------
def _to_dt(s):
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def add_date_column_inplace(df):
    """Vectorized version of pick_date: date = payindat or trndat or docdat"""
    n = len(df)
    payin = df["payindat"] if "payindat" in df.columns else pd.Series([pd.NaT]*n)
    trn   = df["trndat"]   if "trndat"   in df.columns else pd.Series([pd.NaT]*n)
    doc   = df["docdat"]   if "docdat"   in df.columns else pd.Series([pd.NaT]*n)
    df["date"] = payin.fillna(trn).fillna(doc)

# -----------------------------
# READ DBF (faster)
# -----------------------------
def read_dbf(file_path, use_fields=None, date_field=None, start_date=None, end_date=None):
    """
    อ่าน DBF โดย filter วันที่ตั้งแต่ตอน loop
    """
    table = dbf.Table(file_path, codepage='cp874')
    table.open(dbf.READ_ONLY)

    if use_fields is None:
        use_fields = table.field_names

    records = []
    for rec in table:
        row = {f: rec[f] for f in use_fields}  # ใช้ rec[f] แทน __dict__

        # filter ตาม date_field
        if date_field and (start_date or end_date):
            val = row.get(date_field)
            if isinstance(val, (dt.date, dt.datetime)):
                val_date = val if isinstance(val, dt.date) else val.date()
                if start_date and val_date < start_date:
                    continue
                if end_date and val_date > end_date:
                    continue

        records.append(row)

    table.close()

    df = pd.DataFrame(records)

    # แปลง date_field เป็น datetime
    if date_field and date_field in df.columns:
        df[date_field] = pd.to_datetime(df[date_field], errors="coerce")

    return df


def process_aptrn_artrn(file_path, doc_prefixes, type_label, use_fields=None):
    print('--- process_aptrn_artrn ---')
    df = read_dbf(file_path, use_fields=use_fields)
    # to datetime
    df["docdat"] = _to_dt(df.get("DOCDAT"))
    if "DOCDAT" in df.columns:
        df = df.drop(columns=["DOCDAT"])

    # filter prefix + date
    df = df[
        df["DOCNUM"].str.startswith(doc_prefixes, na=False) &
        (df["docdat"].dt.date >= start_date)
    ].copy()

    df["type"] = type_label  # in/out
    df.columns = [c.lower() for c in df.columns]
    return df

def process_bktrn():
    print('--- process_bktrn ---')
    df = read_dbf(dbf_file_bktrn)
    use = ["VOUCHER", "CHQNUM", "TRNDAT", "BNKACC", "PAYINDAT", "NETAMT", "CHQDAT", "REMARK"]
    for c in use:
        if c not in df.columns:
            df[c] = None
    df = df[use].copy()
    df = df.rename(columns={
        "VOUCHER": "bk_voucher",
        "CHQNUM": "chqnum",
        "TRNDAT": "trndat",
        "BNKACC": "bnkacc",
        "PAYINDAT": "payindat",
        "NETAMT": "net_amt",
        "CHQDAT": 'chqdat',
        "REMARK": 'remark',
    })
    # normalize dates now (ช่วย merge/กรองเร็วขึ้น)
    df["trndat"]  = _to_dt(df["trndat"])
    df["payindat"] = _to_dt(df["payindat"])
    df["chqdat"]  = _to_dt(df["chqdat"])
    return df

def merge_with_bktrn(source_df, bktrn_df):
    print('--- merge_with_bktrn ---')
    return pd.merge(source_df, bktrn_df, how="left", left_on="docnum", right_on="bk_voucher")

def clean_chqnum(chq):
    if isinstance(chq, str) and chq.startswith("*"):
        return chq[1:]
    return chq

# (เก็บไว้เผื่อเรียกใช้อื่น ๆ แต่จะไม่ใช้ apply แล้ว)
def pick_date(row):
    if pd.notnull(row["payindat"]):
        return row["payindat"]
    elif pd.notnull(row["trndat"]):
        return row["trndat"]
    else:
        return row["docdat"]

def _strip_object_columns_inplace(df):
    obj_cols = df.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        s = df[col].astype("object")
        try:
            s = s.str.strip()
            s = s.mask(s == "", None)
        except Exception:
            # ถ้าไม่ใช่สตริงจริง ๆ ก็ข้าม
            pass
        df[col] = s

def prepare_final_df(merged_df):
    df = merged_df.copy()
    df = df[df["bnkacc"].notnull()].copy()

    # clean chqnum
    df.loc[:, "chqnum"] = df["chqnum"].apply(clean_chqnum)

    # vectorized date
    add_date_column_inplace(df)

    expected_cols = ["docnum", "docdat", "type", "cuscod", "chqnum",
                     "trndat", "bnkacc", "payindat", "net_amt", "date",
                     "chqdat", "remark"]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    _strip_object_columns_inplace(df)

    final_df = df[expected_cols].copy()

    # enforce Nones (ไม่แปลง dtype ให้พัง)
    for col in ["docnum", "type", "cuscod", "chqnum", "bnkacc", "remark"]:
        final_df.loc[:, col] = final_df[col].where(pd.notnull(final_df[col]), None)

    for col in ["docdat", "trndat", "payindat", "date", "chqdat"]:
        final_df.loc[:, col] = final_df[col].where(pd.notnull(final_df[col]), None)

    final_df.loc[:, "net_amt"] = final_df["net_amt"].where(pd.notnull(final_df["net_amt"]), None)

    return final_df

def process_bkmas():
    print('--- process_bkmas ---')
    df = read_dbf(dbf_file_bkmas)
    use = ["BNKACC", "BNKNAM", "BRANCH", "SHORTNAM", "BNKNUM",
           "BALFWD", "BALDAT", "ACCNUM", "USERID", "CHGDAT", "TAXID", "ORGNUM"]
    for c in use:
        if c not in df.columns:
            df[c] = None
    df = df[use].copy()
    df.columns = [c.lower() for c in df.columns]

    # NaN → None + strip objects
    df = df.where(pd.notnull(df), None)
    _strip_object_columns_inplace(df)
    return df

def process_bktrn_bt(file_path):
    """
    อ่าน BKTRN สำหรับ CHQNUM ที่เริ่มด้วย BT
    และเตรียม DataFrame สำหรับ insert ลง ap_artrn_table
    """
    print('--- process_bktrn_bt ---')
    df = read_dbf(
        file_path,
        use_fields=["CHQNUM", "BKTRNTYP", "TRNDAT", "CHQDAT", "REMARK",
                    "BNKACC", "PAYINDAT", "NETAMT"]
    )

    # filter CHQNUM เริ่มต้นด้วย BT เท่านั้น
    df = df[df["CHQNUM"].str.startswith("BT")].copy()
    if df.empty:
        print("⚠️ No BT records found in BKTRN")
        return pd.DataFrame()

    # DOCNUM = CHQNUM
    df["DOCNUM"] = df["CHQNUM"]

    # TYPE ตาม BKTRNTYP
    def map_type(bktrntyp):
        if bktrntyp == "BT":
            return "out"
        elif bktrntyp.startswith("B"):  # Bx
            return "in"
        else:
            return None
    df["TYPE"] = df["BKTRNTYP"].apply(map_type)

    # แปลงวันที่
    df["TRNDAT"] = pd.to_datetime(df["TRNDAT"], errors="coerce").dt.normalize()
    df["CHQDAT"] = pd.to_datetime(df["CHQDAT"], errors="coerce").dt.normalize()
    df["PAYINDAT"] = pd.to_datetime(df["PAYINDAT"], errors="coerce").dt.normalize()

    # ใช้ pick_date สร้าง date
    df["DATE"] = df.apply(pick_date, axis=1)

    # filter start_date
    df = df[df["TRNDAT"].dt.date >= start_date]

    # select fields สำหรับ insert ลง DB
    fields = ["DOCNUM", "TRNDAT", "TYPE", "CHQNUM", "CHQDAT", "REMARK",
              "BNKACC", "PAYINDAT", "NETAMT", "DATE"]
    df = df[fields].copy()

    # lowercase columns
    df.columns = [c.lower() for c in df.columns]

    return df


def insert_to_db(df, table_name):
    """
    Insert DataFrame into MySQL table.
    - NaN / pd.NA → None
    - ใช้ batch executemany แบบเร็ว (no iterrows)
    """
    print('--- insert_to_db ---', table_name)
    now = pd.Timestamp.today()

    # แปลง NaN → None
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, pd.NaT: None, float("nan"): None})

    with engine.begin() as conn:  # auto-commit/rollback
        if table_name == "ap_artrn_table":
            conn.execute(text("TRUNCATE TABLE ap_artrn_table"))

            sql = text("""
                INSERT INTO ap_artrn_table
                (docnum, docdat, type, cuscod, chqnum, trndat, bnkacc, payindat, net_amt, date, chqdat, remark, created_at)
                VALUES (:docnum, :docdat, :type, :cuscod, :chqnum, :trndat, :bnkacc, :payindat, :net_amt, :date, :chqdat, :remark, :now)
            """)

            data = df.to_dict(orient="records")
            for row in data:
                row["now"] = now

            conn.execute(sql, data)  # executemany
            print(f"✅ Inserted {len(df)} rows into {table_name}")

        elif table_name == "bkmas_table":
            conn.execute(text("TRUNCATE TABLE bkmas_table"))

            sql = text("""
                INSERT INTO bkmas_table
                (bnkacc, bnknam, branch, shortnam, bnknum, balfwd, baldat, accnum, userid, chgdat, taxid, orgnum, created_at)
                VALUES (:bnkacc, :bnknam, :branch, :shortnam, :bnknum, :balfwd, :baldat, :accnum, :userid, :chgdat, :taxid, :orgnum, :now)
            """)

            data = df.to_dict(orient="records")
            for row in data:
                row["now"] = now

            conn.execute(sql, data)
            print(f"✅ Inserted {len(df)} rows into {table_name}")

def task():
    ts = time.time()

    # AP/AR ใช้ table เดียวกัน
    aptrn_df = process_aptrn_artrn(dbf_file_aptrn, ("HI", "HP", "PS", "PN"), "out")
    artrn_df = process_aptrn_artrn(dbf_file_artrn, ("RE", "RC", "RD", "RM", "AI", "AC"), "in")
    bktrn_df = process_bktrn()

    merged_aptrn = merge_with_bktrn(aptrn_df, bktrn_df)
    merged_artrn = merge_with_bktrn(artrn_df, bktrn_df)

    final_aptrn = prepare_final_df(merged_aptrn)
    final_artrn = prepare_final_df(merged_artrn)

    # รวม AP/AR
    combined_df = pd.concat([final_aptrn, final_artrn], ignore_index=True)

    # BKTRN BT
    bktrn_bt_df = process_bktrn_bt(dbf_file_bktrn)
    if not bktrn_bt_df.empty:
        combined_df = pd.concat([combined_df, bktrn_bt_df], ignore_index=True)

    # insert ลง DB
    insert_to_db(combined_df, "ap_artrn_table")

    # BKMAS
    bkmas_df = process_bkmas()
    insert_to_db(bkmas_df, "bkmas_table")

    print(f'================ TOTAL TIME OF PROCESS {time.time() - ts:.2f} SECOND ================')

if __name__ == "__main__":
    print('=============== START PROGRAM =================')
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    time.sleep(1)
    sys.exit()
