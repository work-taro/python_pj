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

start_date = dt.date(2025, 1, 1)
# end_date = pd.Timestamp.today()
# start_date = (end_date - pd.DateOffset(months=2)).replace(day=1)

# -----------------------------
# Utils: fast datetime & date
# -----------------------------
def _to_dt(s):
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def add_date_column_inplace(df):
    """เปลี่ยนให้ใช้แค่ trndat เป็น date"""
    df["date"] = df["trndat"]

# -----------------------------
# READ DBF (faster)
# -----------------------------
def read_dbf(path, codepage="cp874", use_fields=None):
    print('--- read_dbf ---', path)
    table = dbf.Table(path, codepage=codepage)
    table.open(mode=dbf.READ_ONLY)
    if use_fields is None:
        use_fields = table.field_names
    # ✅ ใช้ tuple เร็วกว่า dict-loop
    records = [tuple(rec[field] for field in use_fields) for rec in table]
    table.close()
    return pd.DataFrame.from_records(records, columns=use_fields)

def process_aptrn_artrn(file_path, doc_prefixes, type_label, use_fields=None, is_ap=False):
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

    # ✅ แยก AP/AR
    if is_ap:
        if "supcod" not in df.columns:
            df["supcod"] = None
        df["cuscod"] = None
    else:
        if "cuscod" not in df.columns:
            df["cuscod"] = None
        df["supcod"] = None

    return df


def process_bktrn():
    print('--- process_bktrn ---')
    df = read_dbf(dbf_file_bktrn)
    use = ["VOUCHER", "CHQNUM", "TRNDAT", "BNKACC", "PAYINDAT",
           "NETAMT", "CHQDAT", "REMARK", "CMPLAPP"]
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
        "CMPLAPP": 'cmplapp',
    })
    # normalize dates
    df["trndat"] = _to_dt(df["trndat"])
    df["payindat"] = _to_dt(df["payindat"])
    df["chqdat"] = _to_dt(df["chqdat"])
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

    expected_cols = ["docnum", "docdat", "type", "cuscod", "supcod", "chqnum",
                     "trndat", "bnkacc", "payindat", "net_amt", "date",
                     "chqdat", "remark", "cmplapp"]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    _strip_object_columns_inplace(df)

    final_df = df[expected_cols].copy()

    # enforce Nones
    for col in ["docnum", "type", "cuscod", "supcod", "chqnum", "bnkacc", "remark"]:
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

def process_bktrn_bt(file_path, artrn_df):
    """
    Process BKTRN สำหรับ CHQNUM เริ่มด้วย BT
    - ใส่ cuscod จาก ARTRN
    - supcod = None
    """
    print('--- process_bktrn_bt ---')
    df = read_dbf(file_path)

    # filter CHQNUM เริ่มด้วย BT (กัน NaN)
    df = df[df["CHQNUM"].str.startswith("BT", na=False)].copy()
    if df.empty:
        print("⚠️ No BT records found in BKTRN")
        return pd.DataFrame()

    # DOCNUM = CHQNUM
    df["DOCNUM"] = df["CHQNUM"]

    # map type
    if "BKTRNTYP" not in df.columns:
        df["BKTRNTYP"] = None
    df["TYPE"] = df["BKTRNTYP"].apply(
        lambda x: "out" if x == "BT" else ("in" if isinstance(x, str) and x.startswith("B") else None)
    )

    # normalize dates
    df["TRNDAT"] = _to_dt(df.get("TRNDAT"))
    df["CHQDAT"] = _to_dt(df.get("CHQDAT"))
    df["PAYINDAT"] = _to_dt(df.get("PAYINDAT"))

    # filter by TRNDAT
    df = df[df["TRNDAT"].dt.date >= start_date].copy()

    # ใช้ rule ใหม่
    df["DATE"] = df["TRNDAT"]   # default ใช้ trndat
    df.loc[df["TYPE"] == "in", "DATE"] = df.loc[df["TYPE"] == "in", "CHQDAT"]

    # เติมคอลัมน์ที่ DB ต้องการแต่ BKTRN ไม่มี
    for c in ["BNKACC", "NETAMT", "REMARK", "DOCDAT", "CMPLAPP"]:
        if c not in df.columns:
            df[c] = None

    # map columns -> lowercase + ap_artrn_table schema
    df = df.rename(columns={
        "DOCNUM": "docnum",
        "DOCDAT": "docdat",
        "TYPE": "type",
        "CHQNUM": "chqnum",
        "CUSCOD": "cuscod",
        "TRNDAT": "trndat",
        "BNKACC": "bnkacc",
        "PAYINDAT": "payindat",
        "NETAMT": "net_amt",
        "DATE": "date",
        "CHQDAT": "chqdat",
        "REMARK": "remark",
        "CMPLAPP": "cmplapp",
    })

    # map cuscod จาก ARTRN
    if not artrn_df.empty:
        df = df.merge(artrn_df[['docnum', 'cuscod']], on='docnum', how='left', suffixes=('', '_from_artrn'))
        df['cuscod'] = df['cuscod'].combine_first(df['cuscod_from_artrn'])
        df = df.drop(columns=['cuscod_from_artrn'])

    # BT ไม่มี supcod
    df['supcod'] = None

    _strip_object_columns_inplace(df)
    df = df.where(pd.notnull(df), None)

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
                (docnum, docdat, type, cuscod, supcod, chqnum, trndat, bnkacc, payindat, 
                 net_amt, date, chqdat, remark, cmplapp, created_at)
                VALUES (:docnum, :docdat, :type, :cuscod, :supcod, :chqnum, :trndat, :bnkacc, 
                        :payindat, :net_amt, :date, :chqdat, :remark, :cmplapp, :now)
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
    aptrn_df = process_aptrn_artrn(dbf_file_aptrn, ("HI", "HP", "PS", "PN"), "out", is_ap=True)
    artrn_df = process_aptrn_artrn(dbf_file_artrn, ("RE", "RC", "RD", "RM", "AI", "AC"), "in", is_ap=False)
    bktrn_df = process_bktrn()

    merged_aptrn = merge_with_bktrn(aptrn_df, bktrn_df)
    merged_artrn = merge_with_bktrn(artrn_df, bktrn_df)

    final_aptrn = prepare_final_df(merged_aptrn)
    final_artrn = prepare_final_df(merged_artrn)

    # รวม AP/AR
    combined_df = pd.concat([final_aptrn, final_artrn], ignore_index=True)

    # BKTRN BT
    bktrn_bt_df = process_bktrn_bt(dbf_file_bktrn, final_artrn)
    if not bktrn_bt_df.empty:
        combined_df = pd.concat([combined_df, bktrn_bt_df], ignore_index=True)

    combined_df = combined_df[combined_df["bnkacc"].notnull()].copy()

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
