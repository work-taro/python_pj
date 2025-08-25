import sys, time, unicodedata
import pandas as pd
import dbf
import pymysql
from sqlalchemy import create_engine, text
from threading import Thread

db_username = "root"
db_password = ""
db_host = "localhost"
db_port = "3306"
db_database = "kacee_center"

dbf_file_path_artrn = "C:/Users/kc/Desktop/DBF/ARTRN.DBF"
dbf_file_path_stcrd = "C:/Users/kc/Desktop/DBF/STCRD.DBF"


# ================================ #
#   Common Utils                   #
# ================================ #

def get_db_engine():
    return create_engine(
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4",
        pool_recycle=3600,
        pool_pre_ping=True
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


def fetch_customer_name():
    return pd.read_sql('SELECT cuscod, cusnam FROM ex_customer', get_db_engine())


def get_from_jan_67_months():
    months = list(range(1, 13))
    years = [2024, 2025]
    return months, years


# ================================ #
#   Read ARTRN + STCRD             #
# ================================ #

def read_artrn(dbf_file_path_artrn):
    print('Reading ARTRN file...')
    ts = time.time()
    try:
        table = dbf.Table(filename=dbf_file_path_artrn, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num or len(doc_num) <= 2:
                continue

            prefix = doc_num[:2]
            if prefix in ['OL', 'SL', 'IV', 'SC', 'SR']:
                so_num = record.SONUM.strip() if record.SONUM else None
                if so_num == "":
                    so_num = None

                result.append({
                    'DOCNUM': doc_num,
                    'DOCDAT': record.DOCDAT,
                    'SONUM': so_num,
                    'CUSCOD': str(record.CUSCOD).strip() if record.CUSCOD else None,
                    'AMOUNT': float(record.AMOUNT) if record.AMOUNT else 0.0,
                    'TOTAL': float(record.TOTAL) if record.TOTAL else 0.0,
                    'DOCSTAT': str(record.DOCSTAT).strip() if record.DOCSTAT else None,
                })

        table.close()
        print(f'Read complete: {len(result)} records, Time: {time.time() - ts} sec.')
        return result

    except Exception as e:
        print(f"Error reading DBF file: {e}")
        return []


def read_stcrd(dbf_file_path_stcrd, numbers_main, numbers_credit):
    print('Reading STCRD file...')
    ts = time.time()
    try:
        table = dbf.Table(filename=dbf_file_path_stcrd, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        target_docs = set(numbers_main) | set(numbers_credit)
        result = []
        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num:
                continue
            if doc_num in target_docs:
                result.append({
                    'DOCNUM': doc_num,
                    'DOCDAT': record.DOCDAT,
                    'SEQNUM': getattr(record, 'SEQNUM', None),
                    'STKCOD': str(record.STKCOD).strip() if record.STKCOD else None,
                    'STKDES': str(record.STKDES).strip() if record.STKDES else None,
                    'PEOPLE': str(record.PEOPLE).strip() if record.PEOPLE else None,
                    'FLAG': str(record.FLAG).strip() if record.FLAG else None,
                    'TRNQTY': float(record.TRNQTY) if record.TRNQTY else 0.0,
                    'UNITPR': float(record.UNITPR) if record.UNITPR != 0 else record.TRNVAL
                })

        table.close()
        print(f'Read complete: {len(result)} detail records, Time: {time.time() - ts} sec.')
        return result

    except Exception as e:
        print(f"Error reading STCRD file: {e}")
        return []


def process_stcrd_data(records, numbers_main, numbers_credit):
    print('Processing STCRD data...')
    if not records:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(records)
    main_details = df[df['DOCNUM'].isin(numbers_main)].copy()
    credit_details = df[df['DOCNUM'].isin(numbers_credit)].copy()

    for details in [main_details, credit_details]:
        if 'SEQNUM' in details.columns:
            details['SEQNUM'] = pd.to_numeric(details['SEQNUM'], errors='coerce').fillna(0).astype(int)
        details.sort_values(['DOCNUM', 'STKCOD', 'SEQNUM'], inplace=True)
        details['row_order'] = details.groupby(['DOCNUM', 'STKCOD']).cumcount() + 1

    return main_details, credit_details


# ================================ #
#   Process ARTRN                  #
# ================================ #

def process_artrn_data(records):
    """สำหรับ OL + SL"""
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df['DOCDAT'] = pd.to_datetime(df['DOCDAT'])
    months, years = get_from_jan_67_months()

    ol_df = df[df['DOCNUM'].str.startswith('OL')].copy()
    sl_df = df[df['DOCNUM'].str.startswith('SL')].copy()

    ol_df = ol_df[ol_df['DOCDAT'].dt.year.isin(years) & ol_df['DOCDAT'].dt.month.isin(months)]
    ol_df['DISC'] = ol_df['AMOUNT'] - ol_df['TOTAL']

    ol_df['DOC_NUMBER'] = ol_df['DOCNUM'].str[2:]
    sl_df['DOC_NUMBER'] = sl_df['DOCNUM'].str[2:]

    ol_numbers = set(ol_df['DOC_NUMBER'])
    sl_df = sl_df[sl_df['DOC_NUMBER'].apply(lambda x: any(x.startswith(ol) for ol in ol_numbers))]

    return ol_df, sl_df


def process_artrn_data_iv(records):
    """สำหรับ IV + SC/SR"""
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df['DOCDAT'] = pd.to_datetime(df['DOCDAT'])
    months, years = get_from_jan_67_months()

    iv_df = df[df['DOCNUM'].str.startswith('IV')]
    credit_df = df[df['DOCNUM'].str.startswith(('SC', 'SR'))]

    iv_df = iv_df[iv_df['DOCDAT'].dt.year.isin(years) & iv_df['DOCDAT'].dt.month.isin(months)]
    iv_df['DISC'] = iv_df['AMOUNT'] - iv_df['TOTAL']

    iv_df['DOC_NUMBER'] = iv_df['DOCNUM'].str[2:]
    credit_df = credit_df.copy()
    credit_df['DOC_NUMBER'] = credit_df['DOCNUM'].str[2:]
    credit_df['DOC_TYPE'] = credit_df['DOCNUM'].str[:2]

    iv_numbers = set(iv_df['DOC_NUMBER'])
    credit_df = credit_df[credit_df['DOC_NUMBER'].apply(lambda x: any(x.startswith(iv) for iv in iv_numbers))]

    return iv_df, credit_df


# ================================ #
#   Insert Header + Detail OL/SL   #
# ================================ #

def insert_to_sql(ol_df, sl_df):
    print('Inserting OL and SL header...')
    ts = time.time()
    engine = get_db_engine()
    df_customer = fetch_customer_name()

    ol_df = ol_df.rename(columns={'CUSCOD': 'customer_code'}).merge(df_customer, left_on='customer_code', right_on='cuscod', how='left')
    sl_df = sl_df.rename(columns={'CUSCOD': 'customer_code'}).merge(df_customer, left_on='customer_code', right_on='cuscod', how='left')

    ol_df = ol_df.rename(columns={'cusnam': 'customer_name'})
    sl_df = sl_df.rename(columns={'cusnam': 'customer_name'})

    ol_df['customer_group'] = ol_df.apply(
        lambda row: row['customer_name'] if pd.notna(row['customer_code']) and str(row['customer_code']).startswith('3') else 'อื่นๆ', axis=1
    )
    sl_df['customer_group'] = sl_df.apply(
        lambda row: row['customer_name'] if pd.notna(row['customer_code']) and str(row['customer_code']).startswith('3') else 'อื่นๆ', axis=1
    )

    ol_df = ol_df[['DOCNUM', 'DOCDAT', 'customer_code', 'customer_name', 'customer_group', 'AMOUNT', 'TOTAL', 'DISC', 'DOCSTAT']]
    sl_df = sl_df[['DOCNUM', 'DOCDAT', 'SONUM', 'customer_code', 'customer_name', 'customer_group', 'AMOUNT', 'TOTAL', 'DOCSTAT']]

    ol_df.columns = ['doc_number', 'doc_date', 'customer_code', 'customer_name', 'customer_group', 'amount', 'total', 'discount', 'doc_stat']
    sl_df.columns = ['doc_number', 'doc_date', 'so_number', 'customer_code', 'customer_name', 'customer_group', 'amount', 'total', 'doc_stat']

    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE IF NOT EXISTS artrn_ol_header (
            id INT AUTO_INCREMENT PRIMARY KEY, doc_number VARCHAR(50), doc_date DATE, customer_code VARCHAR(50),
            customer_name TEXT NULL, customer_group TEXT NULL, amount DECIMAL(15,2), total DECIMAL(15,2),
            cost_total DECIMAL(15,2), discount DECIMAL(15,2), doc_stat VARCHAR(10), status BOOLEAN NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))
        connection.execute(text("TRUNCATE TABLE artrn_ol_header"))

        connection.execute(text("""CREATE TABLE IF NOT EXISTS artrn_sl_header (
            id INT AUTO_INCREMENT PRIMARY KEY, doc_number VARCHAR(50), doc_date DATE, so_number VARCHAR(50) NULL,
            customer_code VARCHAR(50), customer_name TEXT NULL, customer_group TEXT NULL,
            amount DECIMAL(15,2), total DECIMAL(15,2), doc_stat VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))
        connection.execute(text("TRUNCATE TABLE artrn_sl_header"))

    if not ol_df.empty:
        ol_df.to_sql('artrn_ol_header', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    if not sl_df.empty:
        sl_df.to_sql('artrn_sl_header', engine, if_exists='append', index=False, chunksize=5000, method='multi')

    print(f"Inserted OL/SL header in {time.time() - ts} sec.")


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

        # ======================== INSERT OL ========================
        if not ol_details.empty:
            ol_details = replace_empty_with_none(ol_details)
            ol_details = ol_details.rename(columns={
                'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum',
                'STKCOD': 'stkcod', 'STKDES': 'stkdes', 'PEOPLE': 'people',
                'FLAG': 'flag', 'TRNQTY': 'trnqty', 'UNITPR': 'unit_price',
                'row_order': 'row_order'
            })
            ol_details = ol_details[['doc_number', 'doc_date', 'seqnum', 'row_order',
                                     'stkcod', 'stkdes', 'people', 'flag', 'trnqty', 'unit_price']]

            print(f"Processing {len(ol_details)} OL detail records")

            with engine.connect() as connection:
                # temp table สำหรับ OL
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_ol_detail"))
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

                # insert chunk
                total_inserted = 0
                for i in range(0, len(ol_details), chunk_size):
                    chunk_df = ol_details.iloc[i:i + chunk_size]
                    chunk_df.to_sql('artrn_temp_ol_detail', con=engine, if_exists='append', index=False)
                    total_inserted += len(chunk_df)
                    print(f"Inserted OL chunk: {i} - {i + len(chunk_df)} (Total {total_inserted})")

                # insert final table พร้อม cost จาก ex_product
                connection.execute(text("""
                    INSERT INTO artrn_ol_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag, 
                        trnqty, unit_price, cost
                    )
                    SELECT t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes, 
                           t.people, t.flag, t.trnqty, t.unit_price, COALESCE(p.unitpr, 0)
                    FROM artrn_temp_ol_detail t
                    LEFT JOIN ex_product p ON t.stkcod = p.stkcod
                """))
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_ol_detail"))

        # ======================== INSERT SL ========================
        if not sl_details.empty:
            sl_details = replace_empty_with_none(sl_details)
            sl_details = sl_details.rename(columns={
                'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum',
                'STKCOD': 'stkcod', 'STKDES': 'stkdes', 'PEOPLE': 'people',
                'FLAG': 'flag', 'TRNQTY': 'trnqty', 'UNITPR': 'unit_price',
                'row_order': 'row_order'
            })
            sl_details = sl_details[['doc_number', 'doc_date', 'seqnum', 'row_order',
                                     'stkcod', 'stkdes', 'people', 'flag', 'trnqty', 'unit_price']]

            print(f"Processing {len(sl_details)} SL detail records")

            with engine.connect() as connection:
                # temp table สำหรับ SL
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_sl_detail"))
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

                # insert chunk
                total_inserted = 0
                for i in range(0, len(sl_details), chunk_size):
                    chunk_df = sl_details.iloc[i:i + chunk_size]
                    chunk_df.to_sql('artrn_temp_sl_detail', con=engine, if_exists='append', index=False)
                    total_inserted += len(chunk_df)
                    print(f"Inserted SL chunk: {i} - {i + len(chunk_df)} (Total {total_inserted})")

                # insert final table พร้อม cost จาก ex_product
                connection.execute(text("""
                    INSERT INTO artrn_sl_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag, 
                        trnqty, unit_price, cost
                    )
                    SELECT t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes, 
                           t.people, t.flag, t.trnqty, t.unit_price, COALESCE(p.unitpr, 0)
                    FROM artrn_temp_sl_detail t
                    LEFT JOIN ex_product p ON t.stkcod = p.stkcod
                """))
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_sl_detail"))

        print('Details SQL Insert Total time: ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error inserting details to SQL: {e}")
        import traceback
        print(traceback.format_exc())


def update_ol_header_detail():
    print("Updating OL ...")
    try:
        engine = get_db_engine()
        with engine.begin() as connection:
            print("Loading OL/SL Detail ...")

            # โหลด detail ของ OL และ SL จาก DB
            ol_detail = pd.read_sql("SELECT * FROM artrn_ol_detail", connection)
            sl_detail = pd.read_sql("SELECT * FROM artrn_sl_detail", connection)

            # เตรียม base_number เอาไว้ใช้ match กัน (เอา substring จาก doc_number)
            ol_detail['base_number'] = ol_detail['doc_number'].str[2:9]
            sl_detail['base_number'] = sl_detail['doc_number'].str[2:9]

            # ฟังก์ชันช่วยหา column ที่น่าจะเป็น description (ชื่อสินค้า)
            def get_description_column(df):
                for col in ['description', 'item_desc', 'desc', 'item_name']:
                    if col in df.columns:
                        return df[col].astype(str).str.strip()
                return pd.Series([""] * len(df))  # fallback เป็นค่าว่าง

            # ฟังก์ชัน normalize string เช่นตัด space แปลกๆ, normalize Unicode
            def normalize_description_column(series):
                return series.astype(str).apply(lambda x: unicodedata.normalize('NFKC', x))\
                    .str.replace(r'\s+', ' ', regex=True).str.strip()

            # ทำ description ให้พร้อมใช้ match
            ol_detail['description'] = normalize_description_column(get_description_column(ol_detail))
            sl_detail['description'] = normalize_description_column(get_description_column(sl_detail))

            # สร้าง key สำหรับ match โดยเอา base_number + stkcod + description มาต่อกัน
            ol_detail['key'] = ol_detail['base_number'] + "|" + ol_detail['stkcod'] + "|" + ol_detail['description']
            sl_detail['key'] = sl_detail['base_number'] + "|" + sl_detail['stkcod'] + "|" + sl_detail['description']

            # รวม qty ของ SL ตาม key
            sl_grouped = sl_detail.groupby('key')['trnqty'].sum().to_dict()

            # เตรียมคอลัมน์ที่จะอัปเดตใน OL detail
            ol_detail['qty_sl_deducted'] = 0.0
            ol_detail['qty_balance'] = ol_detail['trnqty']
            ol_detail['unit_price_from_sl'] = None
            # สำหรับ SL detail track remaining qty ที่ยังใช้ได้
            sl_detail['remaining_qty'] = sl_detail['trnqty']

            # ไล่ loop OL detail เพื่อ match กับ SL detail ทีละรายการ
            for i, row in ol_detail.iterrows():
                key = row['key']
                ol_qty = row['trnqty']
                ol_price = row['unit_price']
                qty_deducted = 0.0
                unit_price_matched = None

                # หา SL detail ที่ key เดียวกัน, ยังมี qty เหลือ, และ unit_price ตรงกัน
                matching_sl = sl_detail[
                    (sl_detail['key'] == key) &
                    (sl_detail['remaining_qty'] > 0) &
                    (sl_detail['unit_price'] == ol_price)
                ]

                # ไล่ลด qty ใน SL detail ตาม OL detail
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

                # อัปเดตค่าที่คำนวณได้ใน OL detail
                ol_detail.at[i, 'qty_sl_deducted'] = qty_deducted
                ol_detail.at[i, 'qty_balance'] = ol_qty - qty_deducted
                ol_detail.at[i, 'unit_price_from_sl'] = unit_price_matched

            print("Updating OL Detail to DB ...")
            # อัปเดตข้อมูลกลับเข้า DB ทีละแถว
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
            # อัปเดต OL header ตาม logic ที่ซับซ้อน (เช็ค cost, status, รวมยอดจาก SL)
            update_ol_header_sql = """
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
            result2 = connection.execute(text(update_ol_header_sql))

            # เซต status = 0 ถ้า NULL
            connection.execute(text("UPDATE artrn_ol_header SET status = 0 WHERE status IS NULL"))

            print("Rows affected (header):", result2.rowcount)

    except Exception as e:
        print(f"Error update ol header: {e}")
        import traceback
        print(traceback.format_exc())


# ================================ #
#   Insert Header + Detail IV/SC   #
# ================================ #

def insert_to_sql_iv(iv_df, credit_df):
    print('Inserting IV and SC/SR header...')
    ts = time.time()
    engine = get_db_engine()
    df_customer = fetch_customer_name()

    iv_df = iv_df.rename(columns={'CUSCOD': 'customer_code'}).merge(df_customer, left_on='customer_code', right_on='cuscod', how='left')
    credit_df = credit_df.rename(columns={'CUSCOD': 'customer_code'}).merge(df_customer, left_on='customer_code', right_on='cuscod', how='left')

    iv_df = iv_df.rename(columns={'cusnam': 'customer_name'})
    credit_df = credit_df.rename(columns={'cusnam': 'customer_name'})

    iv_df['customer_group'] = iv_df.apply(
        lambda row: row['customer_name'] if pd.notna(row['customer_code']) and str(row['customer_code']).startswith('3') else 'อื่นๆ', axis=1
    )
    credit_df['customer_group'] = credit_df.apply(
        lambda row: row['customer_name'] if pd.notna(row['customer_code']) and str(row['customer_code']).startswith('3') else 'อื่นๆ', axis=1
    )

    iv_df = iv_df[['DOCNUM', 'DOCDAT', 'customer_code', 'customer_name', 'customer_group', 'AMOUNT', 'TOTAL', 'DISC', 'DOCSTAT']]
    credit_df = credit_df[['DOCNUM', 'DOCDAT', 'DOC_TYPE', 'customer_code', 'customer_name', 'customer_group', 'AMOUNT', 'TOTAL', 'DOCSTAT']]

    iv_df.columns = ['doc_number', 'doc_date', 'customer_code', 'customer_name', 'customer_group', 'amount', 'total', 'discount', 'doc_stat']
    credit_df.columns = ['doc_number', 'doc_date', 'doc_type', 'customer_code', 'customer_name', 'customer_group', 'amount', 'total', 'doc_stat']

    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE IF NOT EXISTS artrn_iv_header (
            id INT AUTO_INCREMENT PRIMARY KEY, doc_number VARCHAR(50), doc_date DATE, customer_code VARCHAR(50),
            customer_name TEXT NULL, customer_group TEXT NULL, amount DECIMAL(15,2), total DECIMAL(15,2),
            cost_total DECIMAL(15,2), discount DECIMAL(15,2), doc_stat VARCHAR(10), status BOOLEAN NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))
        connection.execute(text("TRUNCATE TABLE artrn_iv_header"))

        connection.execute(text("""CREATE TABLE IF NOT EXISTS artrn_iv_credit_header (
            id INT AUTO_INCREMENT PRIMARY KEY, doc_number VARCHAR(50), doc_date DATE, doc_type VARCHAR(5),
            customer_code VARCHAR(50), customer_name TEXT NULL, customer_group TEXT NULL,
            amount DECIMAL(15,2), total DECIMAL(15,2), doc_stat VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""))
        connection.execute(text("TRUNCATE TABLE artrn_iv_credit_header"))

    if not iv_df.empty:
        iv_df.to_sql('artrn_iv_header', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    if not credit_df.empty:
        credit_df.to_sql('artrn_iv_credit_header', engine, if_exists='append', index=False, chunksize=5000, method='multi')

    print(f"Inserted IV/SC/SR header in {time.time() - ts} sec.")


def insert_details_to_sql_iv(iv_details, credit_details):
    print('Inserting IV and SC/SR details...')
    ts = time.time()
    chunk_size = 1000

    try:
        engine = get_db_engine()

        def replace_empty_with_none(df):
            return df.replace({"": None})

        with engine.connect() as connection:
            # Create IV detail table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_iv_detail (
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
                    qty_credit_deducted DECIMAL(15, 2),
                    qty_balance DECIMAL(15, 2),
                    unit_price_from_credit DECIMAL(15, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("TRUNCATE TABLE artrn_iv_detail"))
            print("IV detail table created and truncated")

            # Create credit (SC, SR) detail table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_iv_credit_detail (
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
                    doc_type VARCHAR(5),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("TRUNCATE TABLE artrn_iv_credit_detail"))
            print("IV credit detail table created and truncated")

        # Insert IV detail
        if not iv_details.empty:
            iv_details = replace_empty_with_none(iv_details)
            iv_details = iv_details.rename(columns={
                'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum',
                'STKCOD': 'stkcod', 'STKDES': 'stkdes', 'PEOPLE': 'people', 'FLAG': 'flag',
                'TRNQTY': 'trnqty', 'UNITPR': 'unit_price', 'row_order': 'row_order'
            })
            iv_details = iv_details[['doc_number', 'doc_date', 'seqnum', 'row_order',
                                     'stkcod', 'stkdes', 'people', 'flag', 'trnqty', 'unit_price']]

            print(f"Processing {len(iv_details)} IV detail records")

            with engine.connect() as connection:
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_iv_detail"))
                connection.execute(text("""
                    CREATE TABLE artrn_temp_iv_detail (
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

                total_inserted = 0
                for i in range(0, len(iv_details), chunk_size):
                    chunk_df = iv_details.iloc[i:i + chunk_size]
                    chunk_df.to_sql('artrn_temp_iv_detail', con=engine, if_exists='append', index=False)
                    total_inserted += len(chunk_df)
                    print(f'Inserted chunk to artrn_temp_iv_detail: {i} to {i + len(chunk_df)} (Total: {total_inserted})')

                connection.execute(text("""
                    INSERT INTO artrn_iv_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag,
                        trnqty, unit_price, cost
                    )
                    SELECT t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes,
                           t.people, t.flag, t.trnqty, t.unit_price,
                           COALESCE(p.unitpr, 0) as cost
                    FROM artrn_temp_iv_detail t
                    LEFT JOIN ex_product p ON t.stkcod = p.stkcod
                """))

                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_iv_detail"))

        # Insert Credit (SC/SR) detail
        if not credit_details.empty:
            credit_details = replace_empty_with_none(credit_details)
            credit_details = credit_details.rename(columns={
                'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum',
                'STKCOD': 'stkcod', 'STKDES': 'stkdes', 'PEOPLE': 'people', 'FLAG': 'flag',
                'TRNQTY': 'trnqty', 'UNITPR': 'unit_price', 'row_order': 'row_order',
                'DOC_TYPE': 'doc_type'  # อาจไม่มีใน DataFrame
            })

            # ถ้าไม่มีคอลัมน์ doc_type ให้สร้างขึ้นมา
            if 'doc_type' not in credit_details.columns:
                credit_details['doc_type'] = None

            credit_details = credit_details[['doc_number', 'doc_date', 'seqnum', 'row_order',
                                             'stkcod', 'stkdes', 'people', 'flag',
                                             'trnqty', 'unit_price', 'doc_type']]

            print(f"Processing {len(credit_details)} credit (SC/SR) detail records")

            with engine.connect() as connection:
                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_iv_credit_detail"))
                connection.execute(text("""
                    CREATE TABLE artrn_temp_iv_credit_detail (
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
                        doc_type VARCHAR(5)
                    )
                """))

                total_inserted = 0
                for i in range(0, len(credit_details), chunk_size):
                    chunk_df = credit_details.iloc[i:i + chunk_size]
                    chunk_df.to_sql('artrn_temp_iv_credit_detail', con=engine, if_exists='append', index=False)
                    total_inserted += len(chunk_df)
                    print(f'Inserted chunk to artrn_temp_iv_credit_detail: {i} to {i + len(chunk_df)} (Total: {total_inserted})')

                connection.execute(text("""
                    INSERT INTO artrn_iv_credit_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag,
                        trnqty, unit_price, cost, doc_type
                    )
                    SELECT t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes,
                           t.people, t.flag, t.trnqty, t.unit_price,
                           COALESCE(p.unitpr, 0) as cost, t.doc_type
                    FROM artrn_temp_iv_credit_detail t
                    LEFT JOIN ex_product p ON t.stkcod = p.stkcod
                """))

                connection.execute(text("DROP TABLE IF EXISTS artrn_temp_iv_credit_detail"))

        print('IV/SC/SR Details SQL Insert Total time: ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error inserting IV/SC/SR details to SQL: {e}")
        import traceback
        print(traceback.format_exc())


def update_iv_header_detail():
    print("Updating IV ...")
    try:
        engine = get_db_engine()
        with engine.begin() as connection: # ใช้ begin() เพื่อให้การเปลี่ยนแปลงถูก commit
            print("Loading IV/credit detail ...")

            # โหลด IV และ Credit detail จาก DB
            iv_detail = pd.read_sql("SELECT * FROM artrn_iv_detail", connection)
            credit_detail = pd.read_sql("SELECT * FROM artrn_iv_credit_detail", connection)

            if iv_detail.empty:
                print("No IV details to update.")
                return

            # base_number = 7 หลักหลัง prefix (IVxx, SCxx, SRxx)
            iv_detail['base_number'] = iv_detail['doc_number'].str[2:9]
            credit_detail['base_number'] = credit_detail['doc_number'].str[2:9]

            # ฟังก์ชันช่วยหา column ที่น่าจะเป็น description (ชื่อสินค้า)
            # ต้องตรวจสอบว่า field 'stkdes' มีอยู่ใน DataFrame หรือไม่
            def get_description_column_robust(df):
                if 'stkdes' in df.columns:
                    return df['stkdes']
                # เพิ่ม fallback ถ้า stkdes ไม่มีใน DF
                elif 'description' in df.columns:
                    return df['description']
                else:
                    return pd.Series([""] * len(df))

            # ฟังก์ชัน normalize string เช่นตัด space แปลกๆ, normalize Unicode
            def normalize_description_column(series):
                return series.astype(str).apply(lambda x: unicodedata.normalize('NFKC', x))\
                    .str.replace(r'\s+', ' ', regex=True).str.strip()

            # ทำ description ให้พร้อมใช้ match
            iv_detail['description'] = normalize_description_column(get_description_column_robust(iv_detail))
            credit_detail['description'] = normalize_description_column(get_description_column_robust(credit_detail))

            # สร้าง key สำหรับ match โดยเอา base_number + stkcod + description มาต่อกัน
            iv_detail['key'] = iv_detail['base_number'] + "|" + iv_detail['stkcod'] + "|" + iv_detail['description']
            credit_detail['key'] = credit_detail['base_number'] + "|" + credit_detail['stkcod'] + "|" + credit_detail['description']

            # เตรียม field สำหรับ update
            iv_detail['qty_credit_deducted'] = 0.0
            iv_detail['qty_balance'] = iv_detail['trnqty'] # เริ่มต้นที่จำนวนเต็มของ IV
            iv_detail['unit_price_from_credit'] = None

            # สำหรับ Credit detail track remaining qty ที่ยังใช้ได้
            # ควรจะหักยอด SR ก่อน แล้วค่อย SC (ถ้า Logic เป็นแบบนั้น) หรือตามลำดับวันที่
            credit_detail['remaining_qty'] = credit_detail['trnqty']


            # Logic การหักยอด IV ด้วย Credit (SC/SR)
            # คล้ายกับ OL ที่หักด้วย SL
            for i, row in iv_detail.iterrows():
                key = row['key']
                iv_qty_current_balance = row['trnqty'] # ยอดเริ่มต้นของ IV item
                iv_price = row['unit_price']
                total_deducted_by_credit = 0.0
                unit_price_matched = None

                # หา Credit detail ที่ key เดียวกัน, ยังมี qty เหลือ, และ unit_price ตรงกัน
                # เรียงตาม doc_date เพื่อให้หักแบบ FIFO หรือตามลำดับที่เหมาะสม
                matching_credit = credit_detail[
                    (credit_detail['key'] == key) &
                    (credit_detail['remaining_qty'] > 0) &
                    (credit_detail['unit_price'] == iv_price) # ควรจะ match ราคาด้วย
                ].sort_values(by='doc_date') # สมมติว่าต้องการ FIFO

                for j, credit_row in matching_credit.iterrows():
                    if total_deducted_by_credit >= iv_qty_current_balance:
                        break # ถ้า IV item ถูกหักครบแล้ว

                    credit_available = credit_row['remaining_qty']
                    to_deduct = min(credit_available, iv_qty_current_balance - total_deducted_by_credit)

                    if to_deduct > 0:
                        # อัปเดต remaining_qty ใน DataFrame ของ credit_detail
                        credit_detail.at[j, 'remaining_qty'] -= to_deduct
                        total_deducted_by_credit += to_deduct

                        if unit_price_matched is None:
                            unit_price_matched = credit_row['unit_price'] # เก็บ unit_price จาก Credit ที่ใช้หัก

                # อัปเดตค่าที่คำนวณได้ใน IV detail
                iv_detail.at[i, 'qty_credit_deducted'] = total_deducted_by_credit
                iv_detail.at[i, 'qty_balance'] = iv_qty_current_balance - total_deducted_by_credit
                iv_detail.at[i, 'unit_price_from_credit'] = unit_price_matched


            # อัปเดตข้อมูล IV Detail กลับเข้า DB
            print("Updating IV Detail to DB ...")
            # ใช้ for loop ในการ update อาจช้า ถ้าข้อมูลเยอะมากแนะนำให้ใช้ batch update/upsert
            for _, row in iv_detail.iterrows():
                connection.execute(text("""
                    UPDATE artrn_iv_detail
                    SET qty_credit_deducted = :deducted,
                        qty_balance = :balance,
                        unit_price_from_credit = :unit_price
                    WHERE id = :id
                """), {
                    "deducted": row['qty_credit_deducted'],
                    "balance": row['qty_balance'],
                    "unit_price": row['unit_price_from_credit'],
                    "id": row['id']
                })

            # ====================================================================
            # อัปเดต IV Header (logic คล้ายกับ OL Header)
            # - cost_total (ถ้า IV มี cost_total ที่ต้องการคำนวณ)
            # - status (ถ้า IV มีสถานะเหมือน OL เช่น 0=มีปัญหา, 1=ปกติ, 2=ปิด)
            # ====================================================================
            print("Updating IV Header ...")
            update_iv_header_sql = """
                UPDATE artrn_iv_header h
                JOIN (
                    SELECT
                        d.doc_number,
                        -- คำนวณ cost_total สำหรับ IV (ถ้าต้องการ)
                        SUM(COALESCE(d.cost, 0.0) * COALESCE(d.qty_balance, 0.0)) AS calculated_cost_total,
                        -- กำหนดสถานะของ IV
                        -- 2 = ปิด (qty_balance = 0)
                        -- 1 = ปกติ (ทุกรายการที่มี qty_balance > 0 มีต้นทุน > 0 หรืออยู่ใน white-list)
                        -- 0 = มีสินค้าที่ไม่มีต้นทุนเหลืออยู่
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
                    FROM artrn_iv_detail d
                    GROUP BY d.doc_number
                ) ds ON h.doc_number = ds.doc_number
                SET
                    h.cost_total = ds.calculated_cost_total,
                    h.status = ds.calculated_status;
            """
            result_header = connection.execute(text(update_iv_header_sql))

            # เซต status = 0 ถ้า NULL สำหรับ IV Header
            connection.execute(text("UPDATE artrn_iv_header SET status = 0 WHERE status IS NULL"))

            print("Rows affected (IV header):", result_header.rowcount)

    except Exception as e:
        print(f"Error update IV header/detail: {e}")
        import traceback
        print(traceback.format_exc())

# ================================ #
#   TASK                           #
# ================================ #

def task():
    print('======= TASK =======')
    total_ts = time.time()

    records = read_artrn(dbf_file_path_artrn)
    if records:
        # ========== OL + SL ==========
        ol_df, sl_df = process_artrn_data(records)
        if not ol_df.empty or not sl_df.empty:
            ol_numbers = set(ol_df['DOCNUM'])
            sl_numbers = set(sl_df['DOCNUM'])
            insert_to_sql(ol_df, sl_df)
            details = read_stcrd(dbf_file_path_stcrd, ol_numbers, sl_numbers)
            if details:
                ol_details, sl_details = process_stcrd_data(details, ol_numbers, sl_numbers)
                insert_details_to_sql(ol_details, sl_details)
                update_ol_header_detail()

        # ========== IV + SC/SR ==========
        iv_df, credit_df = process_artrn_data_iv(records)
        if not iv_df.empty or not credit_df.empty:
            iv_numbers = set(iv_df['DOCNUM'])
            credit_numbers = set(credit_df['DOCNUM'])
            insert_to_sql_iv(iv_df, credit_df)
            details = read_stcrd(dbf_file_path_stcrd, iv_numbers, credit_numbers)
            if details:
                iv_details, credit_details = process_stcrd_data(details, iv_numbers, credit_numbers)
                insert_details_to_sql_iv(iv_details, credit_details)
                update_iv_header_detail()

    print('Total : ' + str(time.time() - total_ts) + ' sec.')


if __name__ == "__main__":
    print('=============== START PROGRAM =================')
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    time.sleep(2)
    sys.exit()
