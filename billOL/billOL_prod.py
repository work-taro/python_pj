import os
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
db_username = 'kaceecent_er'
db_password = '0gC186!S}Bj6rr'
db_port = 3306

dbf_file_path_artrn = '../DBF/ARTRN.DBF'
dbf_file_path_stcrd = '../DBF/STCRD.DBF'


def get_last_3_months():
    """คำนวณช่วง 3 เดือนย้อนหลังจากเดือนปัจจุบัน"""
    today = dt.datetime.now()
    current_month = today.month
    current_year = today.year

    months = []
    years = []

    for i in range(1, 4):  # ย้อนหลัง 3 เดือน
        if current_month - i > 0:
            months.append(current_month - i)
            years.append(current_year)
        else:
            # ถ้าเดือนติดลบ ให้ไปปีที่แล้ว
            months.append(12 + (current_month - i))
            years.append(current_year - 1)

    print(f"Filtering data for months: {months} in years: {list(set(years))}")
    return months, years
    # print("Filtering data for months: [4, 3, 2] in years: [2025]")
    # return [4, 3, 2], [2025]


def get_db_engine():
    return create_engine(
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4",
        pool_recycle=3600,  # รีเซ็ต connection ทุก 1 ชั่วโมง
        pool_pre_ping=True  # ตรวจสอบ connection ก่อนใช้งาน
    )


def safe_float(val):
    if val is None:
        return 0.0
    if isinstance(val, str):
        val = val.strip()
        if val == '':
            return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def read_artrn(dbf_file_path_artrn):
    print('Reading ARTRN file...')
    ts = time.time()

    try:
        table = dbf.Table(filename=dbf_file_path_artrn, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        # อ่านข้อมูลทั้งหมด (ทั้ง OL และ SL)
        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num or len(doc_num) <= 2:
                continue

            # เก็บเฉพาะ OL และ SL
            prefix = doc_num[:2]
            if prefix in ['OL', 'SL']:
                # ถ้า SONUM เป็น empty string ให้เป็น None
                so_num = record.SONUM.strip() if record.SONUM else None
                if so_num == "":
                    so_num = None

                result_append({
                    'DOCNUM': doc_num,
                    'DOCDAT': record.DOCDAT,
                    'SONUM': so_num,
                    'CUSCOD': str(record.CUSCOD).strip() if record.CUSCOD else None,
                    'AMOUNT': float(record.AMOUNT) if record.AMOUNT else 0.0,
                    'TOTAL': float(record.TOTAL) if record.TOTAL else 0.0,
                    'DOCSTAT': str(record.DOCSTAT).strip() if record.DOCSTAT else None,
                })

        table.close()

        print(f'Read complete: {len(result)} records')
        print('Reading time: ' + str(time.time() - ts) + ' sec.')
        return result

    except Exception as e:
        print(f"Error reading DBF file: {e}")
        return []


def process_artrn_data(records):
    """ประมวลผลข้อมูล ARTRN โดยกรองเฉพาะ 3 เดือนย้อนหลัง"""
    print('Processing ARTRN data...')
    ts = time.time()

    try:
        df = pd.DataFrame(records)
        if df.empty:
            print('No records to process')
            return pd.DataFrame(), pd.DataFrame()

        # แปลง data types
        df['DOCDAT'] = pd.to_datetime(df['DOCDAT'])
        df['AMOUNT'] = pd.to_numeric(df['AMOUNT'], errors='coerce').fillna(0.0)
        df['TOTAL'] = pd.to_numeric(df['TOTAL'], errors='coerce').fillna(0.0)

        # แยก DataFrame เป็น OL และ SL
        ol_df = df[df['DOCNUM'].str.startswith('OL')].copy()
        sl_df = df[df['DOCNUM'].str.startswith('SL')].copy()

        # คำนวณ 3 เดือนย้อนหลัง
        months, years = get_last_3_months()

        # กรอง OL ตามเดือนและปีที่ต้องการ
        ol_filtered = pd.DataFrame()
        for year in set(years):  # ใช้ set เพื่อไม่ให้ซ้ำ
            year_data = ol_df[ol_df['DOCDAT'].dt.year == year]
            month_data = year_data[year_data['DOCDAT'].dt.month.isin(months)]
            ol_filtered = pd.concat([ol_filtered, month_data])

        ol_df = ol_filtered

        if ol_df.empty:
            print('No OL records found for specified months')
            return pd.DataFrame(), pd.DataFrame()

        # Recalculate DISC for OL as AMOUNT - TOTAL
        # Ensure AMOUNT and TOTAL are numeric before subtraction
        ol_df['AMOUNT'] = pd.to_numeric(ol_df['AMOUNT'], errors='coerce').fillna(0.0)
        ol_df['TOTAL'] = pd.to_numeric(ol_df['TOTAL'], errors='coerce').fillna(0.0)
        # discount
        ol_df['DISC'] = ol_df['AMOUNT'] - ol_df['TOTAL']

        # สร้าง column เก็บเลขที่เอกสาร
        ol_df['DOC_NUMBER'] = ol_df['DOCNUM'].str[2:]
        sl_df['DOC_NUMBER'] = sl_df['DOCNUM'].str[2:]

        # เก็บเลขที่เอกสาร OL
        ol_numbers = set(ol_df['DOC_NUMBER'])

        # กรอง SL: SONUM ต้องตรงกับ OL เต็มๆ
        sl_df = sl_df[
            sl_df['SONUM'].isin(ol_numbers)
        ]

        # กรอง SL ที่มีเลขตรงกับ OL
        # sl_df = sl_df[sl_df['DOC_NUMBER'].isin(ol_numbers)]

        # กรอง SL ที่มีเลขเอกสารขึ้นต้นเหมือนกับ OL (เช่น OL001 → SL001, SL001-A)
        sl_df = sl_df[sl_df['DOC_NUMBER'].apply(lambda x: any(x.startswith(ol) for ol in ol_numbers))]

        if not sl_df.empty:
            print("\nSL Documents distribution by month:")
            sl_months = sl_df.groupby([sl_df['DOCDAT'].dt.year, sl_df['DOCDAT'].dt.month]).size()
            for (y, m), count in sl_months.items():
                print(f"Year: {y}, Month: {m}, Count: {count}")

        print(f'\nFiltered records: OL: {len(ol_df)}, SL: {len(sl_df)}')

        # จัดเรียง columns ให้ตรงกับ database
        ol_columns = ['DOCNUM', 'DOCDAT', 'CUSCOD', 'AMOUNT', 'TOTAL', 'DISC', 'DOCSTAT']
        sl_columns = ['DOCNUM', 'DOCDAT', 'SONUM', 'CUSCOD', 'AMOUNT', 'TOTAL', 'DOCSTAT']
        ol_df = ol_df[ol_columns]
        sl_df = sl_df[sl_columns]

        return ol_df, sl_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return pd.DataFrame(), pd.DataFrame()


def read_stcrd(dbf_file_path_stcrd, ol_numbers, sl_numbers):
    """อ่านข้อมูล STCRD (detail) ตาม DOCNUM ของ OL และ SL"""
    print('Reading STCRD file...')
    ts = time.time()

    try:
        table = dbf.Table(filename=dbf_file_path_stcrd, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        # รวม document numbers ทั้งหมดที่ต้องการ
        target_docs = set(ol_numbers) | set(sl_numbers)

        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num:
                continue

            # เก็บเฉพาะ records ที่มี DOCNUM ตรงกับ OL หรือ SL
            if doc_num in target_docs:
                result_append({
                    'DOCNUM': doc_num,
                    'DOCDAT': record.DOCDAT,
                    'SEQNUM': record.SEQNUM if hasattr(record, 'SEQNUM') else None,
                    'STKCOD': str(record.STKCOD).strip() if record.STKCOD else None,
                    'STKDES': str(record.STKDES).strip() if record.STKDES else None,
                    'PEOPLE': str(record.PEOPLE).strip() if record.PEOPLE else None,
                    'FLAG': str(record.FLAG).strip() if record.FLAG else None,
                    'TRNQTY': float(record.TRNQTY) if record.TRNQTY else 0.0,
                    'UNITPR': float(record.UNITPR) if record.UNITPR != 0 else record.TRNVAL
                })

        table.close()

        print(f'Read complete: {len(result)} detail records')
        print('Reading time: ' + str(time.time() - ts) + ' sec.')
        return result

    except Exception as e:
        print(f"Error reading STCRD file: {e}")
        return []


def process_stcrd_data(records, ol_numbers, sl_numbers):
    """แยกและประมวลผลข้อมูล STCRD สำหรับ OL และ SL"""
    print('Processing STCRD data...')
    ts = time.time()

    try:
        df = pd.DataFrame(records)
        if df.empty:
            print('No detail records to process')
            return pd.DataFrame(), pd.DataFrame()

        # แยก DataFrame เป็น OL และ SL details
        ol_details = df[df['DOCNUM'].isin(ol_numbers)].copy()
        sl_details = df[df['DOCNUM'].isin(sl_numbers)].copy()

        # Ensure SEQNUM is present and integer (or string if needed)
        if 'SEQNUM' in ol_details.columns:
            ol_details['SEQNUM'] = pd.to_numeric(ol_details['SEQNUM'], errors='coerce').fillna(0).astype(int)
        if 'SEQNUM' in sl_details.columns:
            sl_details['SEQNUM'] = pd.to_numeric(sl_details['SEQNUM'], errors='coerce').fillna(0).astype(int)

        # เพิ่ม row_order (ลำดับบรรทัด) โดย group ตาม DOCNUM, STKCOD แล้ว sort ตาม SEQNUM
        def add_row_order(df):
            if df.empty:
                df['row_order'] = []
                return df
            df = df.sort_values(['DOCNUM', 'STKCOD', 'SEQNUM'])
            df['row_order'] = df.groupby(['DOCNUM', 'STKCOD']).cumcount() + 1
            return df
        ol_details = add_row_order(ol_details)
        sl_details = add_row_order(sl_details)

        print(f'Filtered details: OL: {len(ol_details)}, SL: {len(sl_details)}')
        print('Processing time: ' + str(time.time() - ts) + ' sec.')

        return ol_details, sl_details

    except Exception as e:
        print(f"Error processing STCRD data: {e}")
        return pd.DataFrame(), pd.DataFrame()


def insert_to_sql(ol_df, sl_df):
    print('Inserting OL and SL to SQL...')
    ts = time.time()
    chunk_size = 5000

    try:
        engine = get_db_engine()
        df_customer = fetch_customer_name()
        # เตรียมชื่อ column ให้ตรงกันก่อน merge
        ol_df = ol_df.rename(columns={'CUSCOD': 'customer_code'})
        sl_df = sl_df.rename(columns={'CUSCOD': 'customer_code'})

        # Merge เอาชื่อมาใส่
        ol_df = ol_df.merge(df_customer, left_on='customer_code', right_on='cuscod', how='left')
        sl_df = sl_df.merge(df_customer, left_on='customer_code', right_on='cuscod', how='left')

        # เปลี่ยนชื่อ 'cusnam' → 'customer_name'
        ol_df = ol_df.rename(columns={'cusnam': 'customer_name'})
        sl_df = sl_df.rename(columns={'cusnam': 'customer_name'})

        # Add customer_group field
        ol_df['customer_group'] = ol_df.apply(
            lambda row: row['customer_name'] if pd.notna(row['customer_code']) and str(row['customer_code']).startswith(
                '3') else 'อื่นๆ',
            axis=1
        )
        sl_df['customer_group'] = sl_df.apply(
            lambda row: row['customer_name'] if pd.notna(row['customer_code']) and str(row['customer_code']).startswith(
                '3') else 'อื่นๆ',
            axis=1
        )

        # จัด column ให้ตรงกับ schema
        ol_df = ol_df[
            ['DOCNUM', 'DOCDAT', 'customer_code', 'customer_name', 'customer_group', 'AMOUNT', 'TOTAL', 'DISC',
             'DOCSTAT']]
        sl_df = sl_df[
            ['DOCNUM', 'DOCDAT', 'SONUM', 'customer_code', 'customer_name', 'customer_group', 'AMOUNT', 'TOTAL',
             'DOCSTAT']]

        # ตั้งชื่อ columns ตาม DB
        ol_df.columns = ['doc_number', 'doc_date', 'customer_code', 'customer_name', 'customer_group', 'amount',
                         'total', 'discount',
                         'doc_stat']
        sl_df.columns = ['doc_number', 'doc_date', 'so_number', 'customer_code', 'customer_name', 'customer_group',
                         'amount', 'total',
                         'doc_stat']

        # แปลง empty string เป็น None ก่อน insert
        def replace_empty_with_none(df):
            return df.replace({"": None})

        ol_df = replace_empty_with_none(ol_df)
        sl_df = replace_empty_with_none(sl_df)

        with engine.connect() as connection:
            # Create OL table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_ol_header (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    customer_code VARCHAR(50),
                    customer_name TEXT NULL,
                    customer_group TEXT NULL,
                    amount DECIMAL(15, 2),
                    total DECIMAL(15, 2),
                    cost_total DECIMAL(15, 2),
                    discount DECIMAL(15, 2),
                    doc_stat VARCHAR(10),
                    status BOOLEAN NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("TRUNCATE TABLE artrn_ol_header"))
            print("OL table created and truncated")

            # Create SL table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_sl_header (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    so_number VARCHAR(50) NULL,
                    customer_code VARCHAR(50),
                    customer_name TEXT NULL,
                    customer_group TEXT NULL,
                    amount DECIMAL(15, 2),
                    total DECIMAL(15, 2),
                    doc_stat VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("TRUNCATE TABLE artrn_sl_header"))
            print("SL table created and truncated")

        # Insert OL
        if not ol_df.empty:
            ol_df.columns = ['doc_number', 'doc_date', 'customer_code', 'customer_name', 'customer_group', 'amount',
                             'total', 'discount', 'doc_stat']

            ol_df = replace_empty_with_none(ol_df)  # แปลง empty string เป็น None

            for i in range(0, len(ol_df), chunk_size):
                chunk_df = ol_df.iloc[i:i + chunk_size]
                chunk_df.to_sql('artrn_ol_header',
                                con=engine,
                                if_exists='append',
                                index=False,
                                method='multi',
                                chunksize=chunk_size)
                print(f'Inserted OL records {i} to {i + len(chunk_df)}')

        # Insert SL
        if not sl_df.empty:
            sl_df.columns = ['doc_number', 'doc_date', 'so_number', 'customer_code', 'customer_name', 'customer_group',
                             'amount', 'total',
                             'doc_stat']
            sl_df = replace_empty_with_none(sl_df)  # แปลง empty string เป็น None

            for i in range(0, len(sl_df), chunk_size):
                chunk_df = sl_df.iloc[i:i + chunk_size]
                chunk_df.to_sql('artrn_sl_header',
                                con=engine,
                                if_exists='append',
                                index=False,
                                method='multi',
                                chunksize=chunk_size)
                print(f'Inserted SL records {i} to {i + len(chunk_df)}')

        print('SQL Insert Total time: ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error inserting to SQL: {e}")


def insert_details_to_sql(ol_details, sl_details):
    print('Inserting details to SQL...')
    ts = time.time()
    chunk_size = 1000  # ลดขนาด chunk ลง

    try:
        engine = get_db_engine()

        # แปลง empty string เป็น None
        def replace_empty_with_none(df):
            return df.replace({"": None})

        with engine.connect() as connection:
            # Create OL details table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_ol_detail (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    seqnum INT,
                    row_order INT,
                    stkcod VARCHAR(50),
                    stkdes VARCHAR(255),
                    people VARCHAR(100),
                    flag VARCHAR(10),
                    trnqty DECIMAL(15, 2),
                    unit_price DECIMAL(15, 2),
                    cost DECIMAL(15, 2),
                    qty_sl_deducted DECIMAL(15, 2),
                    qty_balance DECIMAL(15, 2),
                    unit_price_from_sl DECIMAL(15, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("TRUNCATE TABLE artrn_ol_detail"))
            print("OL detail table created and truncated")

            # Create SL details table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_sl_detail (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    seqnum INT,
                    row_order INT,
                    stkcod VARCHAR(50),
                    stkdes VARCHAR(255),
                    people VARCHAR(100),
                    flag VARCHAR(10),
                    trnqty DECIMAL(15, 2),
                    unit_price DECIMAL(15, 2),
                    cost DECIMAL(15, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("TRUNCATE TABLE artrn_sl_detail"))
            print("SL detail table created and truncated")

        # Insert OL details with cost from ex_product
        if not ol_details.empty:
            ol_details = replace_empty_with_none(ol_details)
            # Ensure columns order: doc_number, doc_date, seqnum, stkcod, stkdes, people, flag, trnqty, unit_price
            ol_details = ol_details.rename(columns={'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum', 'STKCOD': 'stkcod', 'STKDES': 'stkdes', 'PEOPLE': 'people', 'FLAG': 'flag', 'TRNQTY': 'trnqty', 'UNITPR': 'unit_price', 'row_order': 'row_order'})
            ol_details = ol_details[['doc_number', 'doc_date', 'seqnum', 'row_order', 'stkcod', 'stkdes', 'people', 'flag', 'trnqty', 'unit_price']]

            print(f"Processing {len(ol_details)} OL detail records")

            # สร้าง temporary table สำหรับ batch insert
            with engine.connect() as connection:
                connection.execute(text("""
                    DROP TABLE IF EXISTS artrn_temp_ol_detail
                """))

                connection.execute(text("""
                    CREATE TABLE artrn_temp_ol_detail (
                        doc_number VARCHAR(50),
                        doc_date DATE,
                        seqnum INT,
                        row_order INT,
                        stkcod VARCHAR(50),
                        stkdes VARCHAR(255),
                        people VARCHAR(100),
                        flag VARCHAR(10),
                        trnqty DECIMAL(15, 2),
                        unit_price DECIMAL(15, 2)
                    )
                """))

                # Insert data เป็น chunks
                total_inserted = 0
                for i in range(0, len(ol_details), chunk_size):
                    try:
                        chunk_df = ol_details.iloc[i:i + chunk_size]
                        chunk_df.to_sql('artrn_temp_ol_detail',
                                        con=engine,
                                        if_exists='append',
                                        index=False)
                        total_inserted += len(chunk_df)
                        print(
                            f'Inserted chunk to artrn_temp_ol_detail: {i} to {i + len(chunk_df)} (Total: {total_inserted})')
                    except Exception as e:
                        print(f"Error inserting chunk {i} to {i + chunk_size}: {e}")
                        engine = get_db_engine()

                # ตรวจสอบจำนวนข้อมูลใน temporary table
                result = connection.execute(text("SELECT COUNT(*) FROM artrn_temp_ol_detail"))
                temp_count = result.scalar()
                print(f"Records in artrn_temp_ol_detail: {temp_count}")

                # ตรวจสอบข้อมูลใน ex_product
                result = connection.execute(text("SELECT COUNT(*) FROM ex_product"))
                product_count = result.scalar()
                print(f"Records in ex_product: {product_count}")

                # Insert into final table with cost from ex_product
                connection.execute(text("""
                    INSERT INTO artrn_ol_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag, 
                        trnqty, unit_price, cost
                    )
                    SELECT 
                        t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes, t.people, t.flag,
                        t.trnqty, t.unit_price, COALESCE(p.unitpr, 0) as cost
                    FROM artrn_temp_ol_detail as t
                    LEFT JOIN ex_product as p ON t.stkcod = p.stkcod
                """))

                # Drop temporary table
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_ol_detail"))

        # Insert SL details with cost from ex_product
        if not sl_details.empty:
            sl_details = replace_empty_with_none(sl_details)
            # Ensure columns order: doc_number, doc_date, seqnum, stkcod, stkdes, people, flag, trnqty, unit_price
            sl_details = sl_details.rename(columns={'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum', 'STKCOD': 'stkcod', 'STKDES': 'stkdes', 'PEOPLE': 'people', 'FLAG': 'flag', 'TRNQTY': 'trnqty', 'UNITPR': 'unit_price', 'row_order': 'row_order'})
            sl_details = sl_details[['doc_number', 'doc_date', 'seqnum', 'row_order', 'stkcod', 'stkdes', 'people', 'flag', 'trnqty', 'unit_price']]

            print(f"Processing {len(sl_details)} SL detail records")

            # สร้าง temporary table สำหรับ batch insert
            with engine.connect() as connection:
                connection.execute(text("""
                    DROP TABLE IF EXISTS artrn_temp_sl_detail
                """))

                connection.execute(text("""
                    CREATE TABLE artrn_temp_sl_detail (
                        doc_number VARCHAR(50),
                        doc_date DATE,
                        seqnum INT,
                        row_order INT,
                        stkcod VARCHAR(50),
                        stkdes VARCHAR(255),
                        people VARCHAR(100),
                        flag VARCHAR(10),
                        trnqty DECIMAL(15, 2),
                        unit_price DECIMAL(15, 2)
                    )
                """))

                # Insert data เป็น chunks
                total_inserted = 0
                for i in range(0, len(sl_details), chunk_size):
                    try:
                        chunk_df = sl_details.iloc[i:i + chunk_size]
                        chunk_df.to_sql('artrn_temp_sl_detail',
                                        con=engine,
                                        if_exists='append',
                                        index=False)
                        total_inserted += len(chunk_df)
                        print(
                            f'Inserted chunk to artrn_temp_sl_detail: {i} to {i + len(chunk_df)} (Total: {total_inserted})')
                    except Exception as e:
                        print(f"Error inserting chunk {i} to {i + chunk_size}: {e}")
                        engine = get_db_engine()

                # ตรวจสอบจำนวนข้อมูลใน temporary table
                result = connection.execute(text("SELECT COUNT(*) FROM artrn_temp_sl_detail"))
                temp_count = result.scalar()
                print(f"Records in artrn_temp_sl_detail: {temp_count}")

                # Insert into final table with cost from ex_product
                connection.execute(text("""
                    INSERT INTO artrn_sl_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag, 
                        trnqty, unit_price, cost
                    )
                    SELECT 
                        t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes, t.people, t.flag,
                        t.trnqty, t.unit_price, COALESCE(p.unitpr, 0) as cost
                    FROM artrn_temp_sl_detail as t
                    LEFT JOIN ex_product as p ON t.stkcod = p.stkcod
                """))

                # ตรวจสอบจำนวนข้อมูลที่ insert สำเร็จ
                result = connection.execute(text("SELECT COUNT(*) FROM artrn_sl_detail"))
                final_count = result.scalar()
                print(f"Records inserted to artrn_sl_detail: {final_count}")

                # Drop temporary table
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_sl_detail"))

                print("Updating ol_detail qty_sl_deducted, balance, unit_price ...")

        print('Details SQL Insert Total time: ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error inserting details to SQL: {e}")
        # แสดง traceback เพื่อดู error ที่ละเอียดขึ้น
        import traceback
        print(traceback.format_exc())


def update_ol_header_detail():
    print("Updating OL ...")
    try:
        engine = get_db_engine()
        with engine.begin() as connection:
            print("Loading OL/SL Detail ...")

            # Load OL and SL detail from DB
            ol_detail = pd.read_sql("SELECT * FROM artrn_ol_detail", connection)
            sl_detail = pd.read_sql("SELECT * FROM artrn_sl_detail", connection)

            # เตรียม base_number
            ol_detail['base_number'] = ol_detail['doc_number'].str[2:9]
            sl_detail['base_number'] = sl_detail['doc_number'].str[2:9]

            # Clean description (เผื่อมี space แปลกๆ เช่น \xa0)
            def get_description_column(df):
                for col in ['description', 'item_desc', 'desc', 'item_name']:
                    if col in df.columns:
                        return df[col].astype(str).str.strip()
                return pd.Series([""] * len(df))  # fallback เป็นค่าว่าง

            def normalize_description_column(series):
                return series.astype(str).apply(lambda x: unicodedata.normalize('NFKC', x)).str.replace(r'\s+', ' ',
                                                                                                        regex=True).str.strip()

            ol_detail['description'] = normalize_description_column(get_description_column(ol_detail))
            sl_detail['description'] = normalize_description_column(get_description_column(sl_detail))

            ol_detail['key'] = ol_detail['base_number'] + "|" + ol_detail['stkcod'] + "|" + ol_detail['description']
            sl_detail['key'] = sl_detail['base_number'] + "|" + sl_detail['stkcod'] + "|" + sl_detail['description']

            # รวม qty SL ตาม key
            sl_grouped = sl_detail.groupby('key')['trnqty'].sum().to_dict()

            # เตรียมฟิลด์ที่จะอัปเดต
            ol_detail['qty_sl_deducted'] = 0.0
            ol_detail['qty_balance'] = ol_detail['trnqty']
            ol_detail['unit_price_from_sl'] = None
            sl_detail['remaining_qty'] = sl_detail['trnqty']

            for i, row in ol_detail.iterrows():
                key = row['key']
                ol_qty = row['trnqty']
                ol_price = row['unit_price']
                qty_deducted = 0.0
                unit_price_matched = None

                matching_sl = sl_detail[
                    (sl_detail['key'] == key) &
                    (sl_detail['remaining_qty'] > 0) &
                    (sl_detail['unit_price'] == ol_price)
                    ]

                for j, sl_row in matching_sl.iterrows():
                    if qty_deducted >= ol_qty:
                        break

                    sl_available = sl_row['remaining_qty']
                    to_deduct = min(sl_available, ol_qty - qty_deducted)

                    if to_deduct > 0:
                        sl_detail.at[j, 'remaining_qty'] -= to_deduct
                        qty_deducted += to_deduct

                        if unit_price_matched is None:
                            unit_price_matched = sl_row['unit_price']

                # ถ้ามี match จริงเท่านั้นถึงจะอัปเดต
                ol_detail.at[i, 'qty_sl_deducted'] = qty_deducted
                ol_detail.at[i, 'qty_balance'] = ol_qty - qty_deducted
                ol_detail.at[i, 'unit_price_from_sl'] = unit_price_matched

            print("Updating OL Detail to DB ...")
            # อัปเดต OL Detail กลับเข้า DB
            for _, row in ol_detail.iterrows():
                connection.execute(text("""
                    UPDATE artrn_ol_detail
                    SET qty_sl_deducted = :deducted,
                        qty_balance = :balance,
                        unit_price_from_sl = :unit_price
                    WHERE id = :id
                """), {
                    "deducted": row['qty_sl_deducted'],
                    "balance": row['qty_balance'],
                    "unit_price": row['unit_price_from_sl'],
                    "id": row['id']
                })

            print("Updating OL Header ...")
            # อัปเดต OL Header ตาม logic ที่กำหนด
            update_ol_header = """
                UPDATE artrn_ol_header h
                JOIN (
                    SELECT
                        d.doc_number,
                        SUM(COALESCE(d.cost, 0.0) * COALESCE(d.qty_balance, 0.0)) AS calculated_cost_total,
                        ol.total AS ol_total,
                        sl_group.sl_total_sum AS sl_total,
                        CASE
                            WHEN COALESCE(SUM(d.qty_balance), 0) = 0 THEN 2
                            WHEN SUM(CASE 
                                        WHEN d.cost = 0 AND d.qty_balance > 0 AND d.stkcod NOT IN (
                                            SELECT sc.stkcod FROM artrn_ol_stkcod_check sc
                                        ) 
                                        THEN 1 
                                        ELSE 0 
                                    END) = 0 THEN 1
                            ELSE 0
                        END AS calculated_status
                    FROM artrn_ol_detail d
                    LEFT JOIN artrn_ol_header ol ON ol.doc_number = d.doc_number
                    LEFT JOIN (
                        SELECT 
                            SUBSTRING(sl.doc_number, 3, 7) AS base_number,
                            SUM(sl.total) AS sl_total_sum
                        FROM artrn_sl_header sl
                        GROUP BY base_number
                    ) sl_group ON sl_group.base_number = SUBSTRING(d.doc_number, 3, 7)
                    GROUP BY d.doc_number
                ) ds ON h.doc_number = ds.doc_number
                SET 
                    h.cost_total = ds.calculated_cost_total,
                    h.status = ds.calculated_status
            """
            result2 = connection.execute(text(update_ol_header))

            # เซต status = 0 ถ้า NULL
            connection.execute(text("UPDATE artrn_ol_header SET status = 0 WHERE status IS NULL"))

            print("Rows affected (header):", result2.rowcount)

    except Exception as e:
        print(f"Error update ol header: {e}")


def fetch_customer_name():
    print('======= Fetch ex_customer SQL =======')
    return pd.read_sql('SELECT cuscod, cusnam FROM ex_customer', get_db_engine())


def task():
    print('======= TASK =======')
    total_ts = time.time()

    # อ่านข้อมูล ARTRN
    records = read_artrn(dbf_file_path_artrn)

    if records:
        # ประมวลผลข้อมูล header
        ol_df, sl_df = process_artrn_data(records)

        if not ol_df.empty or not sl_df.empty:
            # เก็บเลขที่เอกสารสำหรับค้นหาใน STCRD
            ol_numbers = set(ol_df['DOCNUM']) if not ol_df.empty else set()
            sl_numbers = set(sl_df['DOCNUM']) if not sl_df.empty else set()

            # บันทึก header
            insert_to_sql(ol_df, sl_df)

            # อ่านและประมวลผลข้อมูล detail
            detail_records = read_stcrd(dbf_file_path_stcrd, ol_numbers, sl_numbers)

            if detail_records:
                ol_details, sl_details = process_stcrd_data(detail_records, ol_numbers, sl_numbers)
                if not ol_details.empty or not sl_details.empty:
                    insert_details_to_sql(ol_details, sl_details)
                    update_ol_header_detail()

    print('Total : ' + str(time.time() - total_ts) + ' sec.')


def quit_program():
    time.sleep(2)
    os._exit(0)


if __name__ == "__main__":
    print('=============== START PROGRAM =================')
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    quit_program()

