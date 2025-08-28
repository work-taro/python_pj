import sys
import dbf
import time
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, text
from threading import Thread
# ======= CONFIG =======
dbf_file_artrn = "C:/Users/kc/Desktop/DBF/ARTRN.DBF"
dbf_file_bktrn = "C:/Users/kc/Desktop/DBF/BKTRN.DBF"

db_host = "localhost"
db_database = "kacee_center"
db_username = "root"
db_password = ""
db_port = 3306

engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4")

# ======= FUNCTION: อ่าน DBF =======
def read_dbf_artrn():
    print("Reading ARTRN.DBF...")
    table = dbf.Table(dbf_file_artrn, codepage='cp874')
    table.open()

    selected_fields = ['DOCNUM', 'DOCDAT', 'DUEDAT', 'CUSCOD', 'NETVAL', 'REMAMT']
    records = []
    for record in table:
        data = {field: record[field] for field in selected_fields}
        records.append(data)
    table.close()

    df = pd.DataFrame(records)
    if df.empty:
        print("ARTRN is empty")
        return pd.DataFrame()

    # lowercase column
    df.columns = df.columns.str.lower()

    # ✅ strip whitespace
    df['cuscod'] = df['cuscod'].str.strip()

    # ✅ filter docnum
    df = df[df['docnum'].str[:2].isin(['IV', 'CM', 'OL'])]

    # ✅ convert date
    df['docdat'] = pd.to_datetime(df['docdat'], errors='coerce')
    df['duedat'] = pd.to_datetime(df['duedat'], errors='coerce')

    # ✅ convert numeric
    df['netval'] = pd.to_numeric(df['netval'], errors='coerce').fillna(0)
    df['remamt'] = pd.to_numeric(df['remamt'], errors='coerce').fillna(0)

    print(f"Loaded {len(df)} records from ARTRN")
    return df


# ======= FUNCTION: ดึง IV, OL ของลูกค้านั้นๆ จาก artrn_iv_header =======
def get_artrn_from_db(engine):
    print("Fetching IV/CM/OL from database...")
    sql = """
        SELECT 
            doc_number AS docnum,
            doc_date AS docdat,
            duedat AS duedat,
            customer_code AS cuscod,
            amount AS netval,
            remamount AS remamt,
            bilnum
        FROM artrn_iv_header
        WHERE LEFT(doc_number, 2) IN ('IV', 'CM', 'OL')
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        print("No IV/CM/OL records found in database")
        return pd.DataFrame()

    # lowercase column names
    df.columns = df.columns.str.lower()

    # strip strings
    df['cuscod'] = df['cuscod'].astype(str).str.strip()
    df['docnum'] = df['docnum'].astype(str).str.strip()
    df['bilnum'] = df['bilnum'].astype(str).str.strip()

    # convert dates
    df['docdat'] = pd.to_datetime(df['docdat'], errors='coerce')
    df['duedat'] = pd.to_datetime(df['duedat'], errors='coerce')

    # numeric
    df['netval'] = pd.to_numeric(df['netval'], errors='coerce').fillna(0)
    df['remamt'] = pd.to_numeric(df['remamt'], errors='coerce').fillna(0)

    print(f"Loaded {len(df)} records from database")
    return df


# ======= FUNCTION: ดึงลูกค้าจาก ex_customer =======
def get_customers():
    with engine.connect() as conn:
        return pd.read_sql("SELECT cuscod, cusnam, crline, paytrm FROM ex_customer", conn)

# ======= FUNCTION: คำนวณ avg_sales =======
def calculate_avg_sales(artrn_df, cus_df):
    print("Calculating avg_sales (avg per month for last 6 months)...")
    six_months_ago = dt.datetime.today() - pd.DateOffset(months=6)

    # ฟิลเตอร์เฉพาะเอกสาร IV, CM, OL ย้อนหลัง 6 เดือน
    valid_docs = artrn_df[
        (artrn_df['docdat'] >= six_months_ago) &
        (artrn_df['docnum'].str[:2].isin(['IV', 'CM', 'OL']))
    ]

    # sum ยอดขายรวมต่อ customer
    total_sales_df = valid_docs.groupby('cuscod')['netval'].sum().reset_index()

    # หาค่าเฉลี่ยต่อเดือน
    total_sales_df['avg_sales'] = total_sales_df['netval'] / 6
    total_sales_df.drop(columns='netval', inplace=True)

    # merge กับ customer
    merged = cus_df.merge(total_sales_df, how='left', left_on='cuscod', right_on='cuscod')
    merged['avg_sales'] = merged['avg_sales'].fillna(0)

    return merged


# ======= FUNCTION: คำนวณ remaining_amt และ overdue =======
def calculate_remaining(artrn_df):
    print("Calculating remaining amounts...")
    # remaining_amt = sum remamt
    remaining_df = artrn_df.groupby('cuscod')['remamt'].sum().reset_index()
    remaining_df.rename(columns={'remamt': 'remaining_amt'}, inplace=True)

    # overdue = remamt ที่ duedat < วันนี้
    overdue_df = artrn_df[artrn_df['duedat'] < dt.datetime.today()]
    overdue_df = overdue_df.groupby('cuscod')['remamt'].sum().reset_index()
    overdue_df.rename(columns={'remamt': 'remaining_morethan_duedat'}, inplace=True)

    return remaining_df, overdue_df


# ======= FUNCTION: อ่าน BKTRN.DBF และคำนวณ chq_payindat_isnull =======
def calculate_chq_payindat_isnull(dbf_file_bktrn):
    print("Reading BKTRN.DBF...")

    table = dbf.Table(dbf_file_bktrn, codepage='cp874')
    table.open()

    selected_fields = ['CUSCOD', 'CHQNUM', 'PAYINDAT', 'NETAMT']
    records = []
    for rec in table:
        data = {field: rec[field] for field in selected_fields}
        records.append(data)
    table.close()

    df = pd.DataFrame(records)
    if df.empty:
        print("BKTRN is empty")
        return pd.DataFrame()

    # lowercase columns
    df.columns = df.columns.str.lower()

    # strip whitespace
    df['cuscod'] = df['cuscod'].str.strip()

    # ฟิลเตอร์ตามเงื่อนไข
    df = df[
        df['chqnum'].str.startswith('QR') &
        (df['payindat'].isna() | (df['payindat'].astype(str).str.strip() == ''))
        ]

    # convert numeric
    df['netamt'] = pd.to_numeric(df['netamt'], errors='coerce').fillna(0)

    # SUM NETAMT per customer
    sum_df = df.groupby('cuscod')['netamt'].sum().reset_index()
    sum_df.rename(columns={'netamt': 'chq_payindat_isnull'}, inplace=True)

    return sum_df


def calculate_ai_notuse_from_dbf(dbf_file_artrn):
    print("Reading ARTRN.DBF for AI documents...")
    table = dbf.Table(dbf_file_artrn, codepage='cp874')
    table.open()

    selected_fields = ['DOCNUM', 'CUSCOD', 'REMAMT']
    records = []
    for rec in table:
        data = {field: rec[field] for field in selected_fields}
        records.append(data)
    table.close()

    df = pd.DataFrame(records)
    if df.empty:
        print("No AI records found in ARTRN.DBF")
        return pd.DataFrame(columns=['cuscod', 'ai_notuse'])

    # lowercase column
    df.columns = df.columns.str.lower()
    df['cuscod'] = df['cuscod'].str.strip()

    # filter เฉพาะ AI และ REMAMT > 0
    df = df[(df['docnum'].str.startswith('AI')) & (df['remamt'] > 0)]

    # convert numeric
    df['remamt'] = pd.to_numeric(df['remamt'], errors='coerce').fillna(0)

    # group by customer
    ai_sum_df = df.groupby('cuscod')['remamt'].sum().reset_index()
    ai_sum_df.rename(columns={'remamt': 'ai_notuse'}, inplace=True)

    return ai_sum_df


# ======= FUNCTION: รวมข้อมูลและ insert =======
def merge_and_insert(customers, remaining_df, overdue_df):
    print("Merging data...")
    final_df = customers.merge(remaining_df, how='left', on='cuscod')
    final_df = final_df.merge(overdue_df, how='left', on='cuscod')

    final_df['remaining_amt'] = final_df['remaining_amt'].fillna(0)
    final_df['remaining_morethan_duedat'] = final_df['remaining_morethan_duedat'].fillna(0)

    # เพิ่ม chq_payindat_isnull
    chq_df = calculate_chq_payindat_isnull(dbf_file_bktrn)
    final_df = final_df.merge(chq_df, how='left', on='cuscod')
    final_df['chq_payindat_isnull'] = final_df['chq_payindat_isnull'].fillna(0)

    # AI
    ai_df = calculate_ai_notuse_from_dbf(dbf_file_artrn)
    final_df = final_df.merge(ai_df, how='left', on='cuscod')
    final_df['ai_notuse'] = final_df['ai_notuse'].fillna(0)

    final_df['created_at'] = pd.Timestamp.today()

    # เตรียม data สำหรับ insert
    final_df = final_df.rename(columns={
        'cuscod': 'customer_code',
        'cusnam': 'customer_name',
        'crline': 'credit_limit',
        'paytrm': 'paytrm'
    })

    # clear table
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE armas_avg_sales"))

    # insert
    print("Inserting into armas_avg_sales...")
    final_df[['customer_code', 'customer_name', 'avg_sales', 'credit_limit', 'paytrm',
              'remaining_amt', 'remaining_morethan_duedat', 'chq_payindat_isnull', 'ai_notuse', 'created_at']].to_sql(
        'armas_avg_sales', con=engine, if_exists='append', index=False
    )

    print(f"Inserted {len(final_df)} records into armas_avg_sales")

# ======= MAIN TASK =======
def task():
    ts = time.time()
    artrn_df = get_artrn_from_db(engine)

    cus_df = get_customers()
    cus_df['cuscod'] = cus_df['cuscod'].str.strip()

    customers = calculate_avg_sales(artrn_df, cus_df)

    remaining_df, overdue_df = calculate_remaining(artrn_df)

    merge_and_insert(customers, remaining_df, overdue_df)

    print(f"Time to process : {time.time() - ts} sec.")


def quit_program():
    time.sleep(1)
    sys.exit()


# ======= RUN IN THREAD =======
if __name__ == "__main__":
    print("======= START PROGRAM =======")
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print("======= END PROGRAM =======")
    quit_program()
