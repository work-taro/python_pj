import re
import sys
import dbf
import time
import unicodedata
import pandas as pd
import datetime as dt
from threading import Thread
from sqlalchemy import create_engine, text

# Database configuration
db_host = 'localhost'
db_database = 'kacee_center'
db_username = 'root'
db_password = ''
db_port = 3306

def get_db_engine():
    return create_engine(
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4",
        pool_recycle=3600,  # รีเซ็ต connection ทุก 1 ชั่วโมง
        pool_pre_ping=True  # ตรวจสอบ connection ก่อนใช้งาน
    )


def fetch_customer_name():
    engine = get_db_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT cuscod, cusnam, paytrm FROM ex_customer", conn)
    return df

def update_duedat_by_bill():
    print("Updating DUEDAT for IV grouped by BILNUM (paytrm จากลูกค้าเท่านั้น)...")
    try:
        engine = get_db_engine()

        # 1) ดึง IV ที่มี bilnum valid
        sql = """
            SELECT 
                doc_number,
                doc_date,
                customer_code,
                bilnum
            FROM artrn_iv_header
            WHERE LEFT(doc_number, 2) IN ('IV', 'CM', 'OL')
              AND bilnum IS NOT NULL
              AND bilnum <> ''
              AND bilnum <> '~'
        """
        df = pd.read_sql(sql, engine)
        if df.empty:
            print("No IV records with valid BILNUM to update.")
            return

        # 2) map paytrm จาก ex_customer (ใช้ของลูกค้าเท่านั้น)
        cus = fetch_customer_name()[['cuscod', 'paytrm']].rename(
            columns={'cuscod': 'customer_code', 'paytrm': 'paytrm_cus'}
        )
        df = df.merge(cus, on='customer_code', how='left', indicator=True)
        print(df['_merge'].value_counts())

        # ถ้า paytrm_cus ไม่มี → ใช้ 0
        df['paytrm_effective'] = pd.to_numeric(df['paytrm_cus'], errors='coerce').fillna(0).astype(int)

        # 3) เตรียมวันที่
        df['doc_date'] = pd.to_datetime(df['doc_date'], errors='coerce')
        df = df.dropna(subset=['doc_date'])

        # 4) คำนวณ duedat ต่อบิล:
        bill_groups = []
        for billnum, g in df.groupby('bilnum'):
            max_doc_date = g['doc_date'].max()
            # แต่ละบิล สมมติ paytrm ของลูกค้าต้องเท่ากัน
            paytrm_to_use = g['paytrm_effective'].iloc[0]

            print(f"Billnum: {billnum}")
            print(g[['doc_number', 'customer_code', 'paytrm_effective']])
            print(f"MAX doc_date: {max_doc_date}, paytrm_to_use: {paytrm_to_use}")

            if paytrm_to_use == 0:
                duedat = max_doc_date
            else:
                duedat = max_doc_date + pd.Timedelta(days=int(paytrm_to_use))

            bill_groups.append({
                'bilnum': billnum,
                'duedat': duedat
            })

        # 5) อัปเดตกลับ DB
        with engine.begin() as conn:
            for row in bill_groups:
                dued = row['duedat'].date() if hasattr(row['duedat'], 'date') else row['duedat']
                conn.execute(text("""
                    UPDATE artrn_iv_header
                    SET duedat = :duedat
                    WHERE bilnum = :billnum
                      AND LEFT(doc_number, 2) IN ('IV', 'CM', 'OL')
                """), {'duedat': dued, 'billnum': row['bilnum']})

        print(f"DUEDAT update completed for {len(bill_groups)} bill groups.")

    except Exception as e:
        print(f"Error updating DUEDAT by BILNUM: {e}")


update_duedat_by_bill()
