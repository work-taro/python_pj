import dbf
import pandas as pd
import os
from sqlalchemy import create_engine, text
import pymysql
import datetime as dt

# ----------------- CONFIG -----------------
dbf_paths = [
    'C:/Users/kc/Desktop/DBF/STCRD.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD66.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD65.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD64.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD63.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD62.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD61.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD60.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD59.DBF',
    'C:/Users/kc/Desktop/DBF/STCRD57.DBF',

]

db_host = 'localhost'
db_port = 3306
db_database = 'kacee_center'
db_username = 'root'
db_password = ''
table_name = 'dashboard_product_header_date_import'


# ------------------------------------------


def db_connection():
    return pymysql.connect(
        host=db_host,
        user=db_username,
        password=db_password,
        database=db_database,
        port=db_port,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def get_all_stkcod_from_ex_product():
    print("📦 ดึง stkcod จาก ex_product ...")
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT pd.stkcod
                    FROM kacee_center.ex_product as pd
                    LEFT JOIN kacee_center.ex_group as gr ON pd.stkgrp = gr.typcod
                    WHERE gr.typdes IS NOT NULL 
                      AND gr.typdes NOT IN (
                          "รายได้อื่น ๆ",
                          "ทรัพย์สินอื่น ๆ",
                          "บริการค่าซ่อม,ค่าติดตั้ง",
                          "หนี้สิน"
                      );
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                return set(row['stkcod'] for row in rows)
    except Exception as e:
        print(f"❌ ERROR (DB Read): {e}")
        return set()


def extract_earliest_import(stkcod_set, paths):
    earliest_data = {}

    for path in paths:
        file_name = os.path.basename(path)
        print(f"📂 อ่านไฟล์: {file_name}")

        table = dbf.Table(path, default_data_types=dict(C=dbf.Char, D=dbf.Date), codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        for record in table:
            if dbf.is_deleted(record):
                continue

            stkcod = str(record.STKCOD).strip()
            if stkcod not in stkcod_set:
                continue

            docnum = str(record.DOCNUM).strip()
            doc_prefix = docnum[:2]
            if doc_prefix not in ['HI', 'RI', 'RR', 'HP', 'PI', 'JI', 'LL']:
                continue

            doc_date = record.DOCDAT
            if not doc_date:
                continue

            if stkcod not in earliest_data or doc_date < earliest_data[stkcod][0]:
                earliest_data[stkcod] = (doc_date, docnum, file_name)

        table.close()

    # 👇 Ensure all stkcods appear, even if not found in DBF
    df_all = pd.DataFrame(list(stkcod_set), columns=['stkcod'])

    df_found = pd.DataFrame([
        {'stkcod': k, 'earliest_date_import': v[0], 'docnum': v[1][:2], 'source_file': v[2]}
        for k, v in earliest_data.items()
    ])

    df_final = df_all.merge(df_found, on='stkcod', how='left')  # ใส่ null สำหรับที่ไม่เจอ

    return df_final


def insert_to_mysql(df):
    print("📝 กำลัง insert ลง MySQL...")
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_database}?charset=utf8mb4"
        )
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table_name}"))
            conn.execute(text(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1"))
            df['created_at'] = df['updated_at'] = dt.datetime.now()
            df.to_sql(table_name, con=conn, if_exists='append', index=False)
        print("✅ บันทึกสำเร็จ")
    except Exception as e:
        print(f"❌ ERROR (Insert): {e}")


if __name__ == '__main__':
    print("🚀 เริ่มสร้างตาราง earliest_date_import แบบ manual")

    stkcod_set = get_all_stkcod_from_ex_product()
    if not stkcod_set:
        print("🛑 ไม่พบ stkcod ใด ๆ จาก ex_product — ยุติการทำงาน")
        exit(1)

    df = extract_earliest_import(stkcod_set, dbf_paths)
    print(f"📊 เจอข้อมูล {len(df)} stkcod จากไฟล์ STCRD ทั้งหมด")

    insert_to_mysql(df)
    print("🎉 เสร็จสิ้น")
