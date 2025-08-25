import time

import pandas as pd
import dbf
from sqlalchemy import create_engine, text
from datetime import datetime
from threading import Thread
import pymysql

# -------------------------------
# ตั้งค่าการเชื่อมต่อ Database
# -------------------------------
DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'kacee_center',
}
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
)


# -------------------------------
# ฟังก์ชันหลักสำหรับประมวลผล
# -------------------------------


def process():
    print('🔄 Loading DBF file...')
    dbf_path = 'C:/Projects/DBF/ARTRN.DBF'
    table = dbf.Table(dbf_path)
    table.open()

    fields = ['DOCNUM', 'DOCDAT', 'CUSCOD', 'AMOUNT', 'DISC', 'TOTAL']
    records = [{field: rec[field] for field in fields} for rec in table]
    df = pd.DataFrame(records)

    df['DOCDAT'] = pd.to_datetime(df['DOCDAT'], errors='coerce')
    df = df[(df['DOCDAT'] >= pd.Timestamp('2025-04-01')) & (df['DOCDAT'] <= pd.Timestamp.today())]

    df['DOCNUM'] = df['DOCNUM'].astype(str).str.upper().str.strip()
    df['CUSCOD'] = df['CUSCOD'].astype(str).str.strip()

    df_iv = df[df['DOCNUM'].str.startswith('IV') & df['CUSCOD'].str.startswith(('1', '2', '3'))].copy()
    df_credit = df[df['DOCNUM'].str.startswith(('SR', 'SC'))].copy()

    def get_customer_name(cuscod):
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT cusnam FROM ex_customer WHERE cuscod = :cuscod"),
                {'cuscod': cuscod}
            ).fetchone()
            return result[0] if result else None

    def match_credits(iv_doc):
        iv_suffix = iv_doc[2:]

        def normalize(docnum):
            return docnum[2:].split('-')[0]

        return df_credit[df_credit['DOCNUM'].apply(lambda x: normalize(x) == iv_suffix)]

    print(f'🔍 Processing {len(df_iv)} IV records...')
    insert_data = []
    credit_entries = []

    for _, row in df_iv.iterrows():
        iv_doc = row['DOCNUM']
        cuscod = row['CUSCOD']
        matched_credits = match_credits(iv_doc)
        total_credit = matched_credits['TOTAL'].sum() if not matched_credits.empty else 0
        net_total = row['TOTAL'] - total_credit
        customer_name = get_customer_name(cuscod)

        insert_data.append({
            'doc_number': iv_doc,
            'doc_date': row['DOCDAT'],
            'customer_code': cuscod,
            'customer_name': customer_name,
            'amount': row['AMOUNT'],
            'discount': row['DISC'],
            'total': row['TOTAL'],
            'net_total': net_total,
            'debit_note_refs': None if matched_credits.empty else ','.join(matched_credits['DOCNUM'].tolist()),
            'processed_at': datetime.now()
        })

        for _, credit_row in matched_credits.iterrows():
            credit_entries.append({
                'invoice_doc_number': iv_doc,
                'credit_doc_number': credit_row['DOCNUM'],
                'credit_total': credit_row['TOTAL'],
                'credit_date': credit_row['DOCDAT'],
                'created_at': datetime.now()
            })

    print(f"🧮 Total invoice candidates: {len(insert_data)}")
    print(f"🧾 Total credit note matches: {len(credit_entries)}")

    print('📤 Inserting into database...')
    with engine.begin() as conn:
        existing_docs = conn.execute(
            text("SELECT doc_number FROM artrn_invoice")
        ).fetchall()
        existing_doc_set = set(row[0] for row in existing_docs)

        new_records = [item for item in insert_data if item['doc_number'] not in existing_doc_set]
        skipped = len(insert_data) - len(new_records)

        print(f"📦 New invoices to insert: {len(new_records)}")
        print(f"⏭️ Skipped (already exists): {skipped}")

        chunk_size = 500
        for i in range(0, len(new_records), chunk_size):
            conn.execute(
                text("""
                    INSERT INTO artrn_invoice (
                        doc_number, doc_date, customer_code, customer_name,
                        amount, discount, total, net_total,
                        debit_note_refs, processed_at
                    ) VALUES (
                        :doc_number, :doc_date, :customer_code, :customer_name,
                        :amount, :discount, :total, :net_total,
                        :debit_note_refs, :processed_at
                    )
                """),
                new_records[i:i + chunk_size]
            )

        for i in range(0, len(credit_entries), chunk_size):
            conn.execute(
                text("""
                    INSERT INTO artrn_invoice_credit (
                        invoice_doc_number, credit_doc_number, credit_total,
                        credit_date, created_at
                    )
                    VALUES (
                        :invoice_doc_number, :credit_doc_number, :credit_total,
                        :credit_date, :created_at
                    )
                """),
                credit_entries[i:i + chunk_size]
            )

    print(f"✅ Finished! Inserted {len(new_records)} new invoices.")
    print(f"✅ Inserted {len(credit_entries)} credit note records.")


# -------------------------------
# MAIN THREAD
# -------------------------------
if __name__ == "__main__":
    ts = time.time()
    print('=============== START PROGRAM =================')
    thread = Thread(target=process)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    print(f'================ TOTAL TIME {time.time() - ts} sec. ==================')
