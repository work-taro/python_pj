import sys
import dbf
import time
import pandas as pd
import datetime as dt
from threading import Thread
import pymysql.cursors


dbf_file_path_stcrd = 'C:/Users/kc/Desktop/DBF/STCRD.DBF'
dbf_file_path_stmas = 'C:/Users/kc/Desktop/DBF/STMAS.DBF'

db_host = 'localhost'
db_database = 'kacee_center'
db_username = 'root'
db_password = ''
db_port = 3306


def dbCenter():
    conn = pymysql.connect(host=db_host, port=db_port, cursorclass=pymysql.cursors.DictCursor, database=db_database, user=db_username, password=db_password)
    return conn


def process_stcrd(dbf_file_path_stcrd):
    print('Reading STCRD file...')
    ts = time.time()

    try:
        table = dbf.Table(filename=dbf_file_path_stcrd, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        start_date = dt.date(2024, 1, 1)
        end_date = dt.date.today()

        for record in table:
            if dbf.is_deleted(record):
                continue

            doc_num = str(record.DOCNUM).strip()
            posopr = str(record.POSOPR).strip()
            if not doc_num or len(doc_num) <= 2:
                continue

            prefix = doc_num[:2]
            if prefix == 'JU' and posopr == '1':
                doc_date = record.DOCDAT

                if not (start_date <= doc_date <= end_date):
                    continue

                result_append({
                    'DOCNUM': doc_num,
                    'DOCDAT': doc_date,
                    'STKCOD': str(record.STKCOD).strip() if record.STKCOD else None,
                    'TRNQTY': str(record.TRNQTY).strip() if record.TRNQTY else None,
                    'UNITPR': float(record.UNITPR) if record.UNITPR else 0.0,
                    'TRNVAL': float(record.TRNVAL) if record.TRNVAL else 0.0
                })

        table.close()

        print(f'Read complete: {len(result)} records')
        print('Reading time: ' + str(time.time() - ts) + ' sec.')
        process_to_excel(result)
        return result

    except Exception as e:
        print(f"Error reading DBF file: {e}")
        return []


def groupby_sku(df):
    df['TRNQTY'] = pd.to_numeric(df['TRNQTY'], errors='coerce').fillna(0)
    df['UNITPR'] = pd.to_numeric(df['UNITPR'], errors='coerce').fillna(0)

    df_filtered = df[df['UNITPR'] == 0]

    summary_df = df_filtered .groupby('STKCOD', as_index=False).agg({
        'TRNQTY': 'sum',
        'UNITPR': 'sum',
        'TRNVAL': 'sum'
    })

    return summary_df


def tbProductData():
    print('ProductData : Processing...')
    ts = time.time()
    conn = None
    try:
        conn = dbCenter()
        if conn.open:
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            query = "SELECT stkcod, unitpr as unitpr_from_db FROM ex_product"
            cursor.execute(query)
            result = cursor.fetchall()
            print('ProductData : Total : ' + str(time.time() - ts) + ' sec.')
            return pd.DataFrame(result)
    except Exception as e:
        print("DB Error:", e)
        return pd.DataFrame()
    finally:
        if conn and conn.open:
            conn.close()


def read_stmas_file(dbf_file_path_stmas):
    print('Reading STMAS file...')
    ts = time.time()

    try:
        table = dbf.Table(filename=dbf_file_path_stmas, codepage='cp874')
        table.open(mode=dbf.READ_ONLY)

        result = []
        result_append = result.append

        for record in table:
            if dbf.is_deleted(record):
                continue

            stkcod = str(record.STKCOD).strip()
            unitpr = float(record.UNITPR) if record.UNITPR else 0.0
            lpurpr = float(record.LPURPR) if record.LPURPR else 0.0

            result_append({
                'STKCOD_DBF': stkcod,
                'UNITPR_DBF': unitpr,
                'LPURPR_DBF': lpurpr
            })

        table.close()
        print(f'STMAS read complete: {len(result)} records, time: {time.time() - ts:.2f} sec')
        return pd.DataFrame(result)

    except Exception as e:
        print(f"Error reading STMAS: {e}")
        return pd.DataFrame()


def process_to_excel(result):
    print('------- header to excel processing -------')
    df = pd.DataFrame(result)

    # ------------------------
    # database ex_product เอา unitprice จาก DATABASE พี่เอ็ม
    # ------------------------
    # product_df = tbProductData()
    #
    # final_summary = pd.merge(summary_df, product_df, how='left', left_on='STKCOD', right_on='stkcod')
    #
    # final_summary.drop(columns=['stkcod'], inplace=True)
    #
    # final_summary.rename(columns={'unitpr_from_db': 'UNITPR_DBF'}, inplace=True)
    #
    # final_summary = final_summary.reindex(columns=[
    #     'STKCOD',
    #     'TRNQTY',
    #     'UNITPR',
    #     'UNITPR_DBF',
    #     'TRNVAL'
    # ])

    # ------------------------
    # STMAS เอาจาก file DBF โดยตรง
    # ------------------------
    product_df = read_stmas_file(dbf_file_path_stmas)

    # รวม raw data กับ STMAS เพื่อ filter ด้วย UNITPR_DBF
    df_merged = pd.merge(
        df,
        product_df[['STKCOD_DBF', 'UNITPR_DBF']],
        left_on='STKCOD',
        right_on='STKCOD_DBF',
        how='left'
    )

    # filter เฉพาะ STMAS.UNITPR_DBF = 0 สำหรับ Raw Data
    df_raw_filtered = df_merged[df_merged['UNITPR_DBF'] == 0].drop(columns=['STKCOD_DBF'])

    # rename เพื่อให้ชัดว่าเป็นราคาใน STMAS
    df_raw_filtered.rename(columns={'UNITPR_DBF': 'UNITPR_STMAS'}, inplace=True)

    # ทำ group by จากข้อมูลที่ filter แล้ว
    summary_df = groupby_sku(df_raw_filtered)

    # รวม summary กับ STMAS เพื่อดึง UNITPR_STMAS และ LPURPR
    final_summary = pd.merge(
        summary_df,
        product_df,
        how='left',
        left_on='STKCOD',
        right_on='STKCOD_DBF'
    )

    # filter เฉพาะ UNITPR_STMAS = 0 สำหรับ summary ด้วย
    final_summary = final_summary[final_summary['UNITPR_DBF'] == 0]

    # ลบและ rename คอลัมน์ให้ถูกต้อง
    final_summary.drop(columns=['STKCOD_DBF', 'UNITPR'], inplace=True)
    final_summary.rename(columns={
        'UNITPR_DBF': 'UNITPR_STMAS',
        'LPURPR_DBF': 'LPURPR'
    }, inplace=True)

    # จัดลำดับคอลัมน์ที่ต้องการ export
    final_summary = final_summary.reindex(columns=[
        'STKCOD',
        'TRNQTY',
        'UNITPR_STMAS',
        'LPURPR',
        'TRNVAL'
    ])

    # กำหนด path สำหรับ export excel
    export_path = 'C:/Users/kc/Desktop/DBF/report/ju_report/JU_process_20250804.xlsx'

    with pd.ExcelWriter(export_path, engine='xlsxwriter') as writer:
        # export Raw Data
        df_raw_filtered.to_excel(writer, sheet_name='Raw Data', index=False)
        # export Group by SKU
        final_summary.to_excel(writer, sheet_name='Group by SKU', index=False)

    print('Export complete:', export_path)


def task():
    process_stcrd(dbf_file_path_stcrd)
    print('------- TASK PROCESSING -------')


def quit_program():
    time.sleep(2)
    sys.exit(0)


if __name__ == "__main__":
    ts = time.time()
    print('=============== START PROGRAM =================')
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    print(f'=============== TOTAL TIME {time.time() - ts:.2f} sec. ==================')
    quit_program()
