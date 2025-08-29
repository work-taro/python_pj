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

dbf_file_path_artrn = 'C:/Users/kc/desktop/DBF/ARTRN.DBF'
dbf_file_path_stcrd = 'C:/Users/kc/desktop/DBF/STCRD.DBF'
dbf_file_path_artrnrm = 'C:/Users/kc/desktop/DBF/ARTRNRM.DBF'

# end_date = pd.Timestamp.today()
# start_date = (end_date - pd.DateOffset(months=2)).replace(day=1)


end_date = pd.Timestamp(year=2024, month=6, day=30)
start_date = pd.Timestamp(year=2024, month=1, day=1)


def get_last_2_month_range():
    """คืนค่า start_date = 1 ม.ค. 2567 และ end_date = วันนี้"""
    # start_date = pd.Timestamp(year=2024, month=1, day=1)
    # end_date = pd.Timestamp.today()

    """ทีละเดือน"""
    # start_date = pd.Timestamp(year=2025, month=8, day=1)
    # end_date = pd.Timestamp(year=2025, month=8, day=31)

    """คืนค่า start_date และ end_date ของช่วง 2 เดือนย้อนหลังจากวันนี้"""
    end_date = pd.Timestamp.today()
    start_date = (end_date - pd.DateOffset(months=3)).replace(day=1)
    return start_date, end_date


def get_since_2024_JAN():
    months = list(range(1, 13))  # ทุกเดือน
    years = [2024, 2025]  # ปีที่ต้องการ

    # ทีละเดือน
    # months = [6]
    # years = [2025]
    return months, years


# helper สร้างช่วงวันที่จาก months, years
def get_month_range(months, years):
    # ใช้เดือนเดียวก่อน: months[0], years[0]
    y, m = years[0], months[0]
    start = pd.Timestamp(year=y, month=m, day=1)
    end = (start + pd.offsets.MonthEnd(1))  # สิ้นเดือน
    return start, end


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
        # เปิด table และกำหนด encoding
        table = dbf.Table(filename=dbf_file_path_artrn, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num or len(doc_num) <= 2:
                continue

            prefix = doc_num[:2]
            if prefix in ['IV', 'CM', 'SC', 'SR', 'OL', 'SL']:
                so_num = record.SONUM.strip() if record.SONUM else None
                if so_num == "":
                    so_num = None

                # ✅ เก็บเฉพาะฟิลด์ที่ต้องการ
                result_append({
                    'DOCNUM': doc_num,
                    'DOCDAT': str(record.DOCDAT),
                    'SONUM': so_num,
                    'CUSCOD': str(record.CUSCOD).strip() if record.CUSCOD else None,
                    'AMOUNT': float(record.AMOUNT) if record.AMOUNT else 0.0,
                    'TOTAL': float(record.TOTAL) if record.TOTAL else 0.0,
                    'REMAMT': float(record.REMAMT) if record.REMAMT else 0.0,
                    'PAYTRM': int(record.PAYTRM) if record.PAYTRM else 0,
                    'DLVBY': str(record.DLVBY).strip() if record.DLVBY else None,
                    'DOCSTAT': str(record.DOCSTAT).strip() if record.DOCSTAT else None,
                    'BILNUM': str(record.BILNUM).strip() if str(record.BILNUM).strip() != "" and str(record.BILNUM).strip() != "~" else None,
                    'DUEDAT': str(record.DUEDAT) if record.DUEDAT else None
                })

        table.close()

        print(f'Read complete: {len(result)} records')
        print('Reading time: ' + str(time.time() - ts) + ' sec.')
        return result

    except Exception as e:
        print(f"Error reading DBF file: {e}")
        return []


def read_artrnrm(dbf_file_path_artrnrm):
    print('Reading ARTRNRM file...')
    ts = time.time()

    try:
        table = dbf.Table(filename=dbf_file_path_artrnrm, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        # อ่านข้อมูลทั้งหมด (ทั้ง IV, OL และ SC, SR, SL)
        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num or len(doc_num) <= 2:
                continue

            # เก็บเฉพาะ IV, OL และ SC, SR, SL
            prefix = doc_num[:2]
            if prefix in ['IV', 'CM', 'OL']:
                result_append({
                    'DOCNUM': doc_num,
                    'SEQNUM': str(record.SEQNUM) if record.SEQNUM else None,
                    'REMARK': str(record.REMARK) if record.REMARK else None
                })

        table.close()

        print(f'Read complete: {len(result)} records')
        print('Reading time: ' + str(time.time() - ts) + ' sec.')
        return result

    except Exception as e:
        print(f"Error reading DBF file: {e}")
        return []


def process_artrn_data(records, df_customer):
    """ประมวลผลข้อมูล ARTRN โดยกรองเฉพาะ 2 เดือนย้อนหลัง และ join PAYTRM จาก df_customer"""
    print('Processing ARTRN data...')

    try:
        df = pd.DataFrame(records)
        if df.empty:
            print('No records to process')
            return pd.DataFrame(), pd.DataFrame()

        # แปลง data types
        df['DOCDAT'] = pd.to_datetime(df['DOCDAT'], errors='coerce')
        df['AMOUNT'] = pd.to_numeric(df['AMOUNT'], errors='coerce').fillna(0.0)
        df['TOTAL'] = pd.to_numeric(df['TOTAL'], errors='coerce').fillna(0.0)

        # ตรวจสอบ start_date, end_date ต้องประกาศมาก่อน (สมมุติใช้ global)
        df = df[(df['DOCDAT'] >= start_date) & (df['DOCDAT'] <= end_date)]

        # แยก DataFrame เป็น IV, OL และ SC, SR, SL
        iv_df = df[df['DOCNUM'].str.startswith(('IV', 'OL', 'CM'), na=False)].copy()
        sc_sr_df = df[df['DOCNUM'].str.startswith(('SC', 'SR', 'SL'), na=False)].copy()

        if iv_df.empty:
            print('No IV records found for specified months')
            return pd.DataFrame(), pd.DataFrame()

        # Recalculate DISC for IV as AMOUNT - TOTAL
        iv_df['DISC'] = iv_df['AMOUNT'] - iv_df['TOTAL']

        # เก็บเลข IV เต็มๆ สำหรับ match
        iv_numbers = set(iv_df['DOCNUM'])

        # กรอง SC/SR: SONUM ต้องตรงกับ IV เต็มๆ
        sc_sr_df = sc_sr_df[
            sc_sr_df['SONUM'].isin(iv_numbers)
        ]

        if not sc_sr_df.empty:
            print("\nSC, SR Documents distribution by month:")
            sc_sr_months = sc_sr_df.groupby([sc_sr_df['DOCDAT'].dt.year, sc_sr_df['DOCDAT'].dt.month]).size()
            for (y, m), count in sc_sr_months.items():
                print(f"Year: {y}, Month: {m}, Count: {count}")

        print(f'\nFiltered records: IV: {len(iv_df)}, SC|SR: {len(sc_sr_df)}')

        # Join กับ df_customer เพื่อดึง PAYTRM
        # ตรวจสอบว่า df_customer มีคอลัมน์ 'CUSCOD' และ 'PAYTRM'
        if 'CUSCOD' in df_customer.columns and 'PAYTRM' in df_customer.columns:
            iv_df = iv_df.merge(
                df_customer[['CUSCOD', 'PAYTRM']],
                on='CUSCOD',
                how='left',
                suffixes=('', '_cust')
            )
        else:
            iv_df['PAYTRM'] = None  # ถ้าไม่มีข้อมูลลูกค้า ให้ใส่ None

        # จัดการ BILNUM ให้เป็น None ถ้าว่างหรือเป็น ~
        iv_df['BILNUM'] = iv_df['BILNUM'].apply(
            lambda x: str(x).strip() if str(x).strip() not in ["", "~", "None"] else None
        )
        assign_sale_area(iv_df)

        # จัดเรียง columns ให้ตรงกับ database
        iv_columns = ['DOCNUM', 'DOCDAT', 'CUSCOD', 'AMOUNT', 'TOTAL', 'DISC', 'DOCSTAT', 'REMAMT', 'PAYTRM', 'DLVBY', 'BILNUM', 'sale_area', 'DUEDAT']
        sc_sr_columns = ['DOCNUM', 'DOCDAT', 'SONUM', 'CUSCOD', 'AMOUNT', 'TOTAL', 'DOCSTAT']

        # ถ้ามี column ที่ขาดจาก iv_df ให้เติมด้วย None
        for col in iv_columns:
            if col not in iv_df.columns:
                iv_df[col] = None

        iv_df = iv_df[iv_columns]
        sc_sr_df = sc_sr_df[sc_sr_columns]

        return iv_df, sc_sr_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return pd.DataFrame(), pd.DataFrame()


def process_artrnrm(remarks_records):
    """
    รวม REMARK ตาม DOCNUM (เฉพาะ IV/OL/CM) เรียงตาม SEQNUM

    กติกาใหม่:
    - ถ้ามีเลขยาวหลัง '//' (>= 6 หลัก) เช่น //2401018587
        -> ใช้เฉพาะเลขยาว (เต็มตัว) ตามลำดับที่เจอ และ *ไม่ใช้* เลขสั้นเลย
    - ถ้า *ไม่มี* เลขยาว
        -> ค่อย fallback ไปใช้เลขสั้นแบบ //2) //4) เป็น SO2, SO4, ...

    นอกจากนี้ ลบแพทเทิร์น "//ตัวเลข" และ "//ตัวเลข)" ออกจากข้อความ REMARK ที่รวมแล้ว
    """
    import re
    import pandas as pd

    if not remarks_records:
        return pd.DataFrame(columns=['DOCNUM', 'REMARK', 'SO_NUMBER'])

    df = pd.DataFrame(remarks_records)
    df['SEQNUM'] = pd.to_numeric(df.get('SEQNUM', 0), errors='coerce').fillna(0).astype(int)
    df['REMARK'] = df['REMARK'].fillna('').astype(str)

    # --- regex ---
    # สั้น: //  <digits> )
    pat_short = re.compile(r'//\s*(\d+)\)')
    # ยาว:  //  <digits{6,}>  (เช่น 2401018587) รองรับจบด้วยวรรค/ปิดวงเล็บ/จบสตริง
    pat_long  = re.compile(r'//\s*(\d{6,})(?=$|\s|\))')

    # สำหรับลบออกจากข้อความ (ทั้งแบบสั้นและยาว)
    pat_rm = re.compile(r'//\s*\d+\)?')
    # จัดช่องว่าง/คอมมา
    pat_sp = re.compile(r'\s{2,}')
    pat_commas = re.compile(r'\s*,\s*')

    def extract_tokens(text: str):
        """คืน (long_tokens, short_tokens) โดยรักษาลำดับที่เจอ"""
        longs, shorts = [], []
        for m in pat_long.finditer(text):
            longs.append(m.group(1))   # '2401018587'
        for m in pat_short.finditer(text):
            shorts.append(m.group(1))  # '2', '4', ...
        return longs, shorts

    df = df.sort_values(['DOCNUM', 'SEQNUM'])

    out_rows = []
    for doc, group in df.groupby('DOCNUM'):
        remarks = [r for r in group['REMARK'] if r]

        long_tokens_in_order, short_tokens_in_order = [], []
        for r in remarks:
            L, S = extract_tokens(r)
            long_tokens_in_order.extend(L)
            short_tokens_in_order.extend(S)

        # dedupe แบบรักษาลำดับ
        def unique_order(seq):
            return list(dict.fromkeys(seq))

        so_number = None
        if long_tokens_in_order:
            # มีเลขยาว -> ใช้เลขยาวเท่านั้น (เต็มตัว)
            longs = unique_order(long_tokens_in_order)
            # ถ้าอยากเอาเฉพาะตัวแรก: so_number = f"SO{longs[0]}"
            # ถ้าอยากต่อหลายตัว: 'SO123,SO456'
            so_number = f"SO{longs[0]}"
        elif short_tokens_in_order:
            shorts = unique_order(short_tokens_in_order)
            so_number = ','.join(f"SO{s}" for s in shorts)
        else:
            so_number = None

        # ทำความสะอาดข้อความ remark รวม
        cleaned = ' | '.join(remarks) if remarks else ''
        cleaned = pat_rm.sub('', cleaned).strip()
        cleaned = pat_sp.sub(' ', cleaned)
        cleaned = pat_commas.sub(', ', cleaned)
        if cleaned == '':
            cleaned = None

        out_rows.append({
            'DOCNUM': doc,
            'REMARK': cleaned,
            'SO_NUMBER': so_number
        })

    return pd.DataFrame(out_rows, columns=['DOCNUM', 'REMARK', 'SO_NUMBER'])


def read_stcrd(dbf_file_path_stcrd, iv_numbers, sc_sr_numbers):
    """อ่านข้อมูล STCRD (detail) ตาม DOCNUM ของ IV และ SC, SR"""
    print('Reading STCRD file...')
    ts = time.time()

    try:
        table = dbf.Table(filename=dbf_file_path_stcrd, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        # รวม document numbers ทั้งหมดที่ต้องการ
        target_docs = set(iv_numbers) | set(sc_sr_numbers)

        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            if not doc_num:
                continue

            # เก็บเฉพาะ records ที่มี DOCNUM ตรงกับ IV หรือ SC, SR
            if doc_num in target_docs:
                result_append({
                    'DOCNUM': doc_num,
                    'DOCDAT': record.DOCDAT,
                    'SEQNUM': record.SEQNUM if hasattr(record, 'SEQNUM') else None,
                    'STKCOD': str(record.STKCOD).strip() if record.STKCOD else None,
                    'STKDES': str(record.STKDES).strip() if record.STKDES else None,
                    'PEOPLE': str(record.PEOPLE).strip() if record.PEOPLE else None,
                    'FLAG': str(record.FLAG).strip() if record.FLAG else None,
                    'TRNQTY': float(record.TRNQTY) if record.TRNQTY else 0.00,
                    'UNITPR': float(record.UNITPR) if record.UNITPR != 0 else 0.00,
                    'DISC': str(record.DISC) if record.DISC else None,
                    'TRNVAL': float(record.NETVAL) if record.NETVAL else 0.00,
                })

        table.close()

        print(f'Read complete: {len(result)} detail records')
        print('Reading time: ' + str(time.time() - ts) + ' sec.')
        return result

    except Exception as e:
        print(f"Error reading STCRD file: {e}")
        return []


def process_stcrd_data(records, iv_numbers, sc_sr_numbers):
    """แยกและประมวลผลข้อมูล STCRD สำหรับ IV และ SC, SR"""
    print('Processing STCRD data...')
    ts = time.time()

    try:
        df = pd.DataFrame(records)
        if df.empty:
            print('No detail records to process')
            return pd.DataFrame(), pd.DataFrame()

        # แยก DataFrame เป็น IV และ SC, SR details
        iv_details = df[df['DOCNUM'].isin(iv_numbers)].copy()
        sc_sr_details = df[df['DOCNUM'].isin(sc_sr_numbers)].copy()

        # Ensure SEQNUM is present and integer (or string if needed)
        if 'SEQNUM' in iv_details.columns:
            iv_details['SEQNUM'] = pd.to_numeric(iv_details['SEQNUM'], errors='coerce').fillna(0).astype(int)
        if 'SEQNUM' in sc_sr_details.columns:
            sc_sr_details['SEQNUM'] = pd.to_numeric(sc_sr_details['SEQNUM'], errors='coerce').fillna(0).astype(int)

        # เพิ่ม row_order (ลำดับบรรทัด) โดย group ตาม DOCNUM, STKCOD แล้ว sort ตาม SEQNUM
        def add_row_order(df):
            if df.empty:
                df['row_order'] = []
                return df
            df = df.sort_values(['DOCNUM', 'STKCOD', 'SEQNUM'])
            df['row_order'] = df.groupby(['DOCNUM', 'STKCOD']).cumcount() + 1
            return df

        iv_details = add_row_order(iv_details)
        sc_sr_details = add_row_order(sc_sr_details)

        print(f'Filtered details: IV: {len(iv_details)}, SC|SR: {len(sc_sr_details)}')
        print('Processing time: ' + str(time.time() - ts) + ' sec.')

        return iv_details, sc_sr_details

    except Exception as e:
        print(f"Error processing STCRD data: {e}")
        return pd.DataFrame(), pd.DataFrame()


def insert_to_sql(iv_df, sc_sr_df, remarks_df=None, start_date=None, end_date=None, purge="range"):
    """
    purge:
      - "range"  : ลบข้อมูลเดิมในช่วงวันที่ [start_date, end_date] แล้วค่อย insert ใหม่ (แนะนำ)
      - "all"    : ลบทั้งตารางก่อน (ระวัง)
      - "none"   : ไม่ลบ ใช้ upsert ทับเฉพาะรายการที่ชน UNIQUE (พฤติกรรมเดิม)
    """
    print('Inserting IV and SC, SR to SQL...')
    ts = time.time()

    try:
        engine = get_db_engine()
        df_customer = fetch_customer_name()

        # ---- ผูกชื่อลูกค้า + paytrm ----
        iv_df = iv_df.rename(columns={'CUSCOD': 'customer_code'})
        sc_sr_df = sc_sr_df.rename(columns={'CUSCOD': 'customer_code'})

        iv_df = iv_df.merge(
            df_customer[['cuscod', 'cusnam', 'paytrm']],
            left_on='customer_code',
            right_on='cuscod',
            how='left'
        )
        sc_sr_df = sc_sr_df.merge(
            df_customer[['cuscod', 'cusnam']],
            left_on='customer_code',
            right_on='cuscod',
            how='left'
        )

        iv_df.rename(columns={'cusnam': 'customer_name'}, inplace=True)
        sc_sr_df.rename(columns={'cusnam': 'customer_name'}, inplace=True)

        iv_df['paytrm'] = iv_df['paytrm'].fillna(0).astype(int)

        iv_df.drop(columns=['cuscod'], inplace=True)
        sc_sr_df.drop(columns=['cuscod'], inplace=True)

        iv_df['customer_group'] = iv_df['customer_name']
        sc_sr_df['customer_group'] = sc_sr_df['customer_name']

        # ---- ผูก REMARK + SO_NUMBER ----
        if remarks_df is not None and not remarks_df.empty:
            iv_df = iv_df.merge(
                remarks_df.rename(columns={'DOCNUM': 'DOCNUM', 'REMARK': 'remark', 'SO_NUMBER': 'so_number'}),
                on='DOCNUM', how='left'
            )
        else:
            iv_df['remark'] = None
            iv_df['so_number'] = None

        # ------------------ จัดการ IV ------------------ #
        iv_df = iv_df[[
            'DOCNUM', 'DOCDAT', 'customer_code', 'customer_name', 'customer_group',
            'AMOUNT', 'TOTAL', 'DISC', 'DOCSTAT', 'REMAMT', 'paytrm', 'DLVBY', 'BILNUM', 'sale_area', 'DUEDAT',
            'so_number', 'remark'
        ]]
        iv_df.columns = [
            'doc_number', 'doc_date', 'customer_code', 'customer_name', 'customer_group',
            'amount', 'total', 'discount', 'doc_stat', 'remamount', 'paytrm', 'dlvby', 'bilnum', 'sale_area', 'duedat',
            'so_number', 'remark'
        ]
        iv_df = iv_df.replace({"": None}).where(pd.notnull(iv_df), None)
        iv_df = iv_df.drop_duplicates(subset=['doc_number'])

        # ------------------ จัดการ SC, SR ------------------ #
        sc_sr_df = sc_sr_df[[
            'DOCNUM', 'DOCDAT', 'SONUM', 'customer_code', 'customer_name', 'customer_group',
            'AMOUNT', 'TOTAL', 'DOCSTAT'
        ]]
        sc_sr_df.columns = ['doc_number', 'doc_date', 'so_number', 'customer_code',
                            'customer_name', 'customer_group', 'amount', 'total', 'doc_stat']
        sc_sr_df = sc_sr_df.replace({"": None}).where(pd.notnull(sc_sr_df), None)

        with engine.begin() as conn:
            # Create IV table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_iv_header (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    doc_number VARCHAR(50) UNIQUE,
                    doc_date DATE,
                    so_number VARCHAR(100) NULL,
                    customer_code VARCHAR(50),
                    customer_name TEXT NULL,
                    customer_group TEXT NULL,
                    amount DECIMAL(15, 2),
                    remamount DECIMAL(15, 2),
                    total DECIMAL(15, 2),
                    cost_total DECIMAL(15, 2),
                    discount DECIMAL(15, 2),
                    doc_stat VARCHAR(10),
                    status BOOLEAN NULL,
                    paytrm int(11),
                    dlvby VARCHAR(20),
                    remark TEXT NULL,
                    bilnum VARCHAR(30),
                    sale_area_code VARCHAR(10),
                    duedat DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_sc_sr_header (
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

            # ---------- PURGE PHASE: ลบข้อมูลเดิม ----------
            if purge == "all":
                # ลบทั้งตาราง (ระวังใช้งานเฉพาะตอน full rebuild)
                conn.execute(text("TRUNCATE TABLE artrn_iv_header"))
                conn.execute(text("TRUNCATE TABLE artrn_sc_sr_header"))
                print("Purged: TRUNCATE artrn_iv_header, artrn_sc_sr_header")

            elif purge == "range":
                if not (start_date and end_date):
                    raise ValueError("purge='range' ต้องส่ง start_date และ end_date เข้ามาด้วย")

                conn.execute(
                    text("""DELETE FROM artrn_iv_header 
                            WHERE doc_date >= :start_date AND doc_date <= :end_date"""),
                    {"start_date": start_date.date(), "end_date": end_date.date()}
                )
                conn.execute(
                    text("""DELETE FROM artrn_sc_sr_header 
                            WHERE doc_date >= :start_date AND doc_date <= :end_date"""),
                    {"start_date": start_date.date(), "end_date": end_date.date()}
                )
                print(f"Purged by range: {start_date.date()} to {end_date.date()}")

            else:
                # "none" = ไม่ลบ (คงพฤติกรรม upsert เดิม)
                print("Purge skipped (mode='none'). Using upsert only.")

            # ---------- INSERT PHASE ----------
            iv_data = iv_df.to_dict(orient='records')
            if iv_data:
                # ถ้าลบก่อนแล้ว จะใช้ INSERT ธรรมดาหรือ UPSERT ก็ได้
                # เก็บ upsert ไว้เพื่อความยืดหยุ่น/กันกรณีซ้ำบาง doc_number
                insert_iv_query = text("""
                    INSERT INTO artrn_iv_header (
                        doc_number, doc_date, so_number, customer_code, customer_name, customer_group,
                        amount, total, discount, doc_stat, remamount, paytrm, dlvby, remark, bilnum, sale_area_code, duedat
                    ) VALUES (
                        :doc_number, :doc_date, :so_number, :customer_code, :customer_name, :customer_group,
                        :amount, :total, :discount, :doc_stat, :remamount, :paytrm, :dlvby, :remark, :bilnum, :sale_area, :duedat
                    )
                    ON DUPLICATE KEY UPDATE
                        amount = VALUES(amount),
                        total = VALUES(total),
                        discount = VALUES(discount),
                        remamount = VALUES(remamount),
                        paytrm = VALUES(paytrm),
                        dlvby = VALUES(dlvby),
                        remark = VALUES(remark),
                        bilnum = VALUES(bilnum),
                        sale_area_code = VALUES(sale_area_code),
                        duedat = VALUES(duedat),
                        so_number = VALUES(so_number)
                """)
                conn.execute(insert_iv_query, iv_data)

            sc_data = sc_sr_df.to_dict(orient='records')
            if sc_data:
                insert_sc_query = text("""
                    INSERT INTO artrn_sc_sr_header (
                        doc_number, doc_date, so_number, customer_code,
                        customer_name, customer_group, amount, total, doc_stat
                    ) VALUES (
                        :doc_number, :doc_date, :so_number, :customer_code,
                        :customer_name, :customer_group, :amount, :total, :doc_stat
                    )
                """)
                conn.execute(insert_sc_query, sc_data)

            print(f"Inserted/Updated {len(iv_data)} IV records")
            print(f"Inserted {len(sc_data)} SC/SR records")

        print('SQL Insert Total time: ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error inserting to SQL: {e}")

def insert_details_to_sql(iv_details, sc_sr_details):
    print('Inserting details to SQL...')
    ts = time.time()
    chunk_size = 1000  # ลดขนาด chunk ลง

    # ทีละเดือน
    # start_date = pd.Timestamp(year=2025, month=8, day=1)

    # ย้อนหลัง 2 เดือน
    # end_date = pd.Timestamp.today()
    # start_date = (end_date - pd.DateOffset(months=3)).replace(day=1)

    try:
        engine = get_db_engine()

        # แปลง empty string เป็น None
        def replace_empty_with_none(df):
            return df.replace({"": None})

        with engine.begin() as conn:
            # Create IV details table
            conn.execute(text("""
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
                    discount VARCHAR(50),
                    trnval DECIMAL(15, 2),
                    cost DECIMAL(15, 2),
                    qty_sc_sr_deducted DECIMAL(15, 2),
                    qty_balance DECIMAL(15, 2),
                    unit_price_from_sc_sr DECIMAL(15, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            # ลบแล้ว running ใหม่ตั้งแต่ 67
            # conn.execute(text("TRUNCATE TABLE artrn_iv_detail"))
            # ลบย้อนหลัง 2 เดือน
            conn.execute(
                text("""DELETE FROM artrn_iv_detail 
                        WHERE doc_date >= :start_date
                            AND doc_date <= :end_date
                        """),
                {
                    "start_date": start_date.date(),
                    "end_date": end_date.date()
                }
            )
            print("IV detail table created and truncated")
            # Create SC, SR details table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS artrn_sc_sr_detail (
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
                    discount VARCHAR(50),
                    trnval DECIMAL(15, 2),
                    cost DECIMAL(15, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            # ลบแล้ว running ใหม่ตั้งแต่ 67
            # conn.execute(text("TRUNCATE TABLE artrn_sc_sr_detail"))
            # ลบย้อนหลัง 2 เดือน
            conn.execute(
                text("""DELETE FROM artrn_sc_sr_detail 
                        WHERE doc_date >= :start_date
                            AND doc_date <= :end_date
                        """),
                {
                    "start_date": start_date.date(),
                    "end_date": end_date.date()
                }
            )
            print("SC, SR detail table created and truncated")

        # Insert IV details with cost from ex_product
        if not iv_details.empty:
            iv_details = replace_empty_with_none(iv_details)
            # Ensure columns order: doc_number, doc_date, seqnum, stkcod, stkdes, people, flag, trnqty, unit_price
            iv_details = iv_details.rename(
                columns={'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum', 'STKCOD': 'stkcod',
                         'STKDES': 'stkdes', 'PEOPLE': 'people', 'FLAG': 'flag', 'TRNQTY': 'trnqty',
                         'UNITPR': 'unit_price', 'DISC': 'discount', 'TRNVAL': 'trnval', 'row_order': 'row_order'})
            iv_details = iv_details[
                ['doc_number', 'doc_date', 'seqnum', 'row_order', 'stkcod', 'stkdes', 'people', 'flag', 'trnqty',
                 'unit_price', 'discount', 'trnval']]

            print(f"Processing {len(iv_details)} IV detail records")

            # สร้าง temporary table สำหรับ batch insert
            with engine.begin() as conn:
                conn.execute(text("""
                    DROP TABLE IF EXISTS artrn_temp_iv_detail
                """))
                conn.execute(text("""
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
                        unit_price DECIMAL(15, 2),
                        discount VARCHAR(50),
                        trnval DECIMAL(15, 2)
                    )
                """))

                # Insert data เป็น chunks
                total_inserted = 0
                for i in range(0, len(iv_details), chunk_size):
                    try:
                        chunk_df = iv_details.iloc[i:i + chunk_size]
                        chunk_df.to_sql('artrn_temp_iv_detail',
                                        con=engine,
                                        if_exists='append',
                                        index=False)
                        total_inserted += len(chunk_df)
                        print(
                            f'Inserted chunk to artrn_temp_iv_detail: {i} to {i + len(chunk_df)} (Total: {total_inserted})')
                    except Exception as e:
                        print(f"Error inserting chunk {i} to {i + chunk_size}: {e}")
                        engine = get_db_engine()

                # ตรวจสอบจำนวนข้อมูลใน temporary table
                result = conn.execute(text("SELECT COUNT(*) FROM artrn_temp_iv_detail"))
                temp_count = result.scalar()
                print(f"Records in artrn_temp_iv_detail: {temp_count}")

                # ตรวจสอบข้อมูลใน ex_product
                result = conn.execute(text("SELECT COUNT(*) FROM ex_product"))
                product_count = result.scalar()
                print(f"Records in ex_product: {product_count}")
                # Insert into final table with cost from ex_product
                conn.execute(text("""
                    INSERT INTO artrn_iv_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag, 
                        trnqty, unit_price, discount, trnval, cost
                    )
                    SELECT 
                        t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes, t.people, t.flag,
                        t.trnqty, t.unit_price, t.discount, t.trnval, COALESCE(p.unitpr, 0) as cost
                    FROM artrn_temp_iv_detail as t
                    LEFT JOIN ex_product as p ON t.stkcod = p.stkcod
                """))

                # Drop temporary table
                conn.execute(text("DROP TABLE IF EXISTS artrn_temp_iv_detail"))

        # Insert SC, SR details with cost from ex_product
        if not sc_sr_details.empty:
            sc_sr_details = replace_empty_with_none(sc_sr_details)
            # Ensure columns order: doc_number, doc_date, seqnum, stkcod, stkdes, people, flag, trnqty, unit_price
            sc_sr_details = sc_sr_details.rename(
                columns={'DOCNUM': 'doc_number', 'DOCDAT': 'doc_date', 'SEQNUM': 'seqnum', 'STKCOD': 'stkcod',
                         'STKDES': 'stkdes', 'PEOPLE': 'people', 'FLAG': 'flag', 'TRNQTY': 'trnqty',
                         'UNITPR': 'unit_price', 'DISC': 'discount', 'TRNVAL': 'trnval', 'row_order': 'row_order'})
            sc_sr_details = sc_sr_details[
                ['doc_number', 'doc_date', 'seqnum', 'row_order', 'stkcod', 'stkdes', 'people', 'flag', 'trnqty',
                 'unit_price', 'discount', 'trnval']]

            print(f"Processing {len(sc_sr_details)} SC, SR detail records")

            # สร้าง temporary table สำหรับ batch insert
            with engine.begin() as conn:
                conn.execute(text("""
                    DROP TABLE IF EXISTS artrn_temp_sc_sr_detail
                """))
                conn.execute(text("""
                    CREATE TABLE artrn_temp_sc_sr_detail (
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
                        discount VARCHAR(50),
                        trnval DECIMAL(15, 2)
                    )
                """))

                # Insert data เป็น chunks
                total_inserted = 0
                for i in range(0, len(sc_sr_details), chunk_size):
                    try:
                        chunk_df = sc_sr_details.iloc[i:i + chunk_size]
                        chunk_df.to_sql('artrn_temp_sc_sr_detail',
                                        con=engine,
                                        if_exists='append',
                                        index=False)
                        total_inserted += len(chunk_df)
                        print(
                            f'Inserted chunk to artrn_temp_sc_sr_detail: {i} to {i + len(chunk_df)} (Total: {total_inserted})')
                    except Exception as e:
                        print(f"Error inserting chunk {i} to {i + chunk_size}: {e}")
                        engine = get_db_engine()

                # ตรวจสอบจำนวนข้อมูลใน temporary table
                result = conn.execute(text("SELECT COUNT(*) FROM artrn_temp_sc_sr_detail"))
                temp_count = result.scalar()
                print(f"Records in artrn_temp_sc_sr_detail: {temp_count}")
                # Insert into final table with cost from ex_product
                conn.execute(text("""
                    INSERT INTO artrn_sc_sr_detail (
                        doc_number, doc_date, seqnum, row_order, stkcod, stkdes, people, flag, 
                        trnqty, unit_price, discount, trnval, cost
                    )
                    SELECT 
                        t.doc_number, t.doc_date, t.seqnum, t.row_order, t.stkcod, t.stkdes, t.people, t.flag,
                        t.trnqty, t.unit_price, t.discount, t.trnval, COALESCE(p.unitpr, 0) as cost
                    FROM artrn_temp_sc_sr_detail as t
                    LEFT JOIN ex_product as p ON t.stkcod = p.stkcod
                """))

                # ตรวจสอบจำนวนข้อมูลที่ insert สำเร็จ
                result = conn.execute(text("SELECT COUNT(*) FROM artrn_sc_sr_detail"))
                final_count = result.scalar()
                print(f"Records inserted to artrn_sc_sr_detail: {final_count}")

                # Drop temporary table
                conn.execute(text("DROP TABLE IF EXISTS artrn_temp_sc_sr_detail"))

                print("Updating iv_detail qty_sc_sr_deducted, balance, unit_price ...")

        print('Details SQL Insert Total time: ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error inserting details to SQL: {e}")
        # แสดง traceback เพื่อดู error ที่ละเอียดขึ้น
        import traceback
        print(traceback.format_exc())


def update_iv_header_detail():
    print("Updating IV ...")
    try:
        engine = get_db_engine()

        # มกรา 67
        # months, years = get_since_2024_JAN()
        # start, end = get_month_range(months, years)

        # ย้อนหลัง 2 เดือน
        # start, end = get_last_2_month_range()

        start, end = start_date, end_date
        with engine.begin() as conn:
            print("Loading IV,OL,CM / SC,SR,SL Detail ...")

            # Load IV, OL and SC, SR, SL detail from DB
            iv_detail = pd.read_sql(text("""
                           SELECT id, doc_number, doc_date, stkcod, trnqty, unit_price, cost, qty_balance,
                                  COALESCE(qty_sc_sr_deducted, 0) AS qty_sc_sr_deducted
                           FROM artrn_iv_detail
                           WHERE doc_date >= :start AND doc_date <= :end
                       """), conn, params={"start": start.date(), "end": end.date()})

            sc_sr_detail = pd.read_sql(text("""
                           SELECT doc_number, doc_date, stkcod, trnqty, unit_price
                           FROM artrn_sc_sr_detail
                           WHERE doc_date >= :start AND doc_date <= :end
                       """), conn, params={"start": start.date(), "end": end.date()})

            # เตรียม base_number
            iv_detail['base_number'] = iv_detail['doc_number'].str[2:9]
            sc_sr_detail['base_number'] = sc_sr_detail['doc_number'].str[2:9]

            # Clean description
            def get_description_column(df):
                for col in ['description', 'item_desc', 'desc', 'item_name']:
                    if col in df.columns:
                        return df[col].astype(str).str.strip()
                return pd.Series([""] * len(df))

            def normalize_description_column(series):
                return series.astype(str).apply(
                    lambda x: unicodedata.normalize('NFKC', x)
                ).str.replace(r'\s+', ' ', regex=True).str.strip()

            iv_detail['description'] = normalize_description_column(get_description_column(iv_detail))
            sc_sr_detail['description'] = normalize_description_column(get_description_column(sc_sr_detail))

            iv_detail['key'] = iv_detail['base_number'] + "|" + iv_detail['stkcod'] + "|" + iv_detail['description']
            sc_sr_detail['key'] = sc_sr_detail['base_number'] + "|" + sc_sr_detail['stkcod'] + "|" + sc_sr_detail[
                'description']

            # รวม qty SC, SR ตาม key
            sc_sr_grouped = sc_sr_detail.groupby('key')['trnqty'].sum().to_dict()

            # เตรียมฟิลด์ที่จะอัปเดต
            iv_detail['qty_sc_sr_deducted'] = 0.0
            iv_detail['qty_balance'] = iv_detail['trnqty']
            iv_detail['unit_price_from_sc_sr'] = None
            sc_sr_detail['remaining_qty'] = sc_sr_detail['trnqty']

            for i, row in iv_detail.iterrows():
                key = row['key']
                iv_qty = row['trnqty']
                iv_price = row['unit_price']
                qty_deducted = 0.0
                unit_price_matched = None

                matching_sc_sr = sc_sr_detail[
                    (sc_sr_detail['key'] == key) &
                    (sc_sr_detail['remaining_qty'] > 0) &
                    (sc_sr_detail['unit_price'] == iv_price)
                    ]

                for j, sc_sr_row in matching_sc_sr.iterrows():
                    if qty_deducted >= iv_qty:
                        break

                    sc_sr_available = sc_sr_row['remaining_qty']
                    to_deduct = min(sc_sr_available, iv_qty - qty_deducted)

                    if to_deduct > 0:
                        sc_sr_detail.at[j, 'remaining_qty'] -= to_deduct
                        qty_deducted += to_deduct

                        if unit_price_matched is None:
                            unit_price_matched = sc_sr_row['unit_price']

                iv_detail.at[i, 'qty_sc_sr_deducted'] = qty_deducted
                iv_detail.at[i, 'qty_balance'] = iv_qty - qty_deducted
                iv_detail.at[i, 'unit_price_from_sc_sr'] = unit_price_matched

            print("Updating IV Detail to DB ...")
            for _, row in iv_detail.iterrows():
                conn.execute(text("""
                    UPDATE artrn_iv_detail
                    SET qty_sc_sr_deducted = :deducted,
                        qty_balance = :balance,
                        unit_price_from_sc_sr = :unit_price
                    WHERE id = :id
                """), {
                    "deducted": row['qty_sc_sr_deducted'],
                    "balance": row['qty_balance'],
                    "unit_price": row['unit_price_from_sc_sr'],
                    "id": row['id']
                })

            print("Updating IV Header in batches ...")
            BATCH_SIZE = 1000
            doc_numbers = pd.read_sql("SELECT DISTINCT doc_number FROM artrn_iv_detail", conn)[
                'doc_number'].tolist()

            for i in range(0, len(doc_numbers), BATCH_SIZE):
                batch_docs = doc_numbers[i:i + BATCH_SIZE]
                conn.execute(text("""
                    UPDATE artrn_iv_header h
                    JOIN (
                        SELECT
                            d.doc_number,
                            SUM(COALESCE(d.cost, 0.0) * COALESCE(d.qty_balance, 0.0)) AS calculated_cost_total,
                            iv.total AS iv_total,
                            sc_sr_group.sc_sr AS sc_sr_total,
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
                        LEFT JOIN artrn_iv_header iv ON iv.doc_number = d.doc_number
                        LEFT JOIN (
                            SELECT
                                SUBSTRING(sc_sr.doc_number, 3, 7) AS base_number,
                                SUM(sc_sr.total) AS sc_sr
                            FROM artrn_sc_sr_header sc_sr
                            GROUP BY base_number
                        ) sc_sr_group ON sc_sr_group.base_number = SUBSTRING(d.doc_number, 3, 7)
                        WHERE d.doc_number IN :batch
                        GROUP BY d.doc_number
                    ) ds ON h.doc_number = ds.doc_number
                    SET
                        h.cost_total = ds.calculated_cost_total,
                        h.status = ds.calculated_status
                """), {"batch": tuple(batch_docs)})

            # เซต status = 0 ถ้า NULL
            conn.execute(text("UPDATE artrn_iv_header SET status = 0 WHERE status IS NULL"))

            print("Finished updating IV header.")

    except Exception as e:
        print(f"Error update iv header: {e}")


def assign_sale_area(iv_df):
    """
    iv_df: DataFrame ของ IV ที่มีฟิลด์ 'cuscod'
    """
    # ดึงข้อมูล sale_area table
    sale_area_df = fetch_check_sale_area()

    # เรียงให้ cuscod_startwith ยาวสุดก่อน (กรณี prefix ซ้อนกัน)
    sale_area_df = sale_area_df.sort_values('cuscod_startwith', key=lambda x: x.str.len(), ascending=False)

    # ฟังก์ชันหาค่า sale_area สำหรับ cuscod แต่ละตัว
    def get_sale_area(cuscod):
        for _, row in sale_area_df.iterrows():
            if cuscod.startswith(row['cuscod_startwith']):
                return row['sale_area']
        return None  # ถ้าไม่ match

    # สร้างคอลัมน์ใหม่
    iv_df['sale_area'] = iv_df['CUSCOD'].apply(get_sale_area)
    return iv_df


def fetch_customer_name():
    engine = get_db_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT cuscod, cusnam, paytrm FROM ex_customer", conn)
    return df

def fetch_check_sale_area():
    engine = get_db_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT sale_area, cuscod_startwith FROM artrn_iv_check_sale_area", conn)
    return df

def _to_pydate(x):
    if pd.isna(x):
        return None
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    if isinstance(x, (pd.Timestamp, dt.datetime)):
        return x.date()
    # เผื่อมาเป็นสตริง
    return pd.to_datetime(x, errors='coerce').date()

def update_duedat_by_bill():
    """
    อัปเดต DUEDAT ของ IV, CM, OL ตาม BILNUM
    วิธีทำ:
      1) GROUP BY bilnum
      2) หาค่า MAX(doc_date) ของแต่ละบิล
      3) + paytrm ของลูกค้า (field ที่ insert ลงตอนแรก)
      4) UPDATE กลับตาราง artrn_iv_header
    """
    print("Updating DUEDAT for IV grouped by BILNUM...")

    try:
        engine = get_db_engine()

        # ดึง MAX(doc_date) และ paytrm ต่อ bilnum
        df = pd.read_sql("""
            SELECT bilnum, MAX(doc_date) AS max_doc_date, MAX(paytrm) AS paytrm
            FROM artrn_iv_header
            WHERE LEFT(doc_number, 2) IN ('IV', 'CM', 'OL')
              AND bilnum IS NOT NULL
              AND bilnum <> ''
              AND bilnum <> '~'
            GROUP BY bilnum
        """, engine)

        if df.empty:
            print("No IV records with valid BILNUM to update.")
            return

        with engine.begin() as conn:
            for _, row in df.iterrows():
                max_date = row['max_doc_date']
                paytrm_days = int(row['paytrm']) if row['paytrm'] else 0
                duedat = max_date + pd.Timedelta(days=paytrm_days)

                conn.execute(text("""
                    UPDATE artrn_iv_header
                    SET duedat = :duedat
                    WHERE bilnum = :bilnum
                      AND LEFT(doc_number, 2) IN ('IV', 'CM', 'OL')
                """), {"duedat": duedat, "bilnum": row['bilnum']})

        print(f"DUEDAT update completed for {len(df)} bill groups.")

    except Exception as e:
        print(f"Error updating DUEDAT by BILNUM: {e}")

def task():
    print('======= TASK =======')
    total_ts = time.time()

    # อ่านข้อมูล ARTRN (header ดิบ)
    records = read_artrn(dbf_file_path_artrn)
    df_customer = fetch_customer_name()

    if records:
        # ประมวลผล header (IV/OL/CM, SC/SR/SL) ตามช่วงวันที่
        iv_df, sc_sr_df = process_artrn_data(records, df_customer)

        if not iv_df.empty or not sc_sr_df.empty:
            # อ่าน REMARK จาก ARTRNRM แล้วประมวลผล
            remarks_records = read_artrnrm(dbf_file_path_artrnrm)
            remarks_df = process_artrnrm(remarks_records) if remarks_records else pd.DataFrame()

            # เก็บเลขที่เอกสารสำหรับ STCRD
            iv_numbers = set(iv_df['DOCNUM']) if not iv_df.empty else set()
            sc_sr_numbers = set(sc_sr_df['DOCNUM']) if not sc_sr_df.empty else set()

            # บันทึก header (ส่ง remarks_df เข้าไป)
            insert_to_sql(
                iv_df, sc_sr_df,
                remarks_df=remarks_df,
                start_date=start_date,
                end_date=end_date,
                purge="range"  # หรือ "all" ถ้าต้องการล้างทั้งตารางก่อน
            )

            update_duedat_by_bill()
            # อ่านและประมวลผล detail
            detail_records = read_stcrd(dbf_file_path_stcrd, iv_numbers, sc_sr_numbers)

            if detail_records:
                iv_details, sc_sr_details = process_stcrd_data(detail_records, iv_numbers, sc_sr_numbers)
                if not iv_details.empty or not sc_sr_details.empty:
                    insert_details_to_sql(iv_details, sc_sr_details)
                    update_iv_header_detail()

    print('Total : ' + str(time.time() - total_ts) + ' sec.')


def quit_program():
    time.sleep(1)
    sys.exit()


if __name__ == "__main__":
    print('=============== START PROGRAM =================')
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    quit_program()
