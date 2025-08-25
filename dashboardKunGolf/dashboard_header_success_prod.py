from threading import Thread
from sqlalchemy import create_engine, text
import numpy as np
import pandas as pd
import dbf
import time
import datetime as dt
import pymysql.cursors
import os

dbf_file_path_stmas = '../DBF/STMAS.DBF'
dbf_file_path_stcrd = '../DBF/STCRD.DBF'

db_host = 'localhost'
db_database = 'kacee_center'
db_username = 'kaceecent_er'
db_password = '0gC186!S}Bj6rr'
db_port = 3306


def dbCenter():
    conn = pymysql.connect(host=db_host, port=db_port, cursorclass=pymysql.cursors.DictCursor, database=db_database,
                           user=db_username, password=db_password)
    return conn


def tbProductData():
    print('ProductData : Processing...')
    ts = time.time()
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            query = """
                        SELECT pd.stkcod, pd.stkdes, pd.barcod as barcode, acc.accdes as category, gr.typdes as `group`, 
                                u.typdes as unit, odf.owner as owner_emp_id
                        FROM kacee_center.ex_product as pd
                        LEFT JOIN kacee_center.ex_account as acc ON pd.acccod = acc.acccod
                        LEFT JOIN kacee_center.ex_group as gr ON pd.stkgrp = gr.typcod
                        LEFT JOIN kacee_center.ex_unit as u ON pd.qucod = u.typcod
                        LEFT JOIN kacee_center.od_flag as odf ON pd.stkcod = odf.sku
                        LEFT JOIN kacee_center.users as user ON odf.owner = user.emp_id
                        WHERE gr.typdes IS NOT NULL 
                          AND gr.typdes NOT IN ("รายได้อื่น ๆ", "ทรัพย์สินอื่น ๆ", "บริการค่าซ่อม,ค่าติดตั้ง", "หนี้สิน");
                        """
            cursor.execute(query)
            result = cursor.fetchall()
            print('ProductData : Total : ' + str(time.time() - ts) + ' sec.')
            return result
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def module_stcrd(dbf_file_path_stcrd, df_purchase_date):
    print('STCRD : Processing...')
    ts = time.time()
    current_month = dt.datetime.now().month
    current_year = dt.datetime.now().year
    last_month = (current_month - 1) if current_month > 1 else 12
    last_month_year = current_year if current_month > 1 else current_year - 1

    table = dbf.Table(filename=dbf_file_path_stcrd, default_data_types=dict(C=dbf.Char, D=dbf.Date), codepage='cp874')
    table.open(mode=dbf.READ_ONLY)

    latest_withdraw_date = {}
    earliest_receive_date = {}
    stcrd_record = []

    for record in table:
        if dbf.is_deleted(record):
            continue

        stkcod = str(record.STKCOD)
        doc_date = record.DOCDAT
        trnqty = float(record.TRNQTY)
        docnum_prefix = str(record.DOCNUM).strip()[0:2]

        # เบิกออก

        if docnum_prefix in ['IV', 'OL', 'OU', 'PD', 'OC', 'OW', 'OO']:
            if stkcod not in latest_withdraw_date or doc_date > latest_withdraw_date[stkcod][0]:
                latest_withdraw_date[stkcod] = (doc_date, str(record.DOCNUM)[0:2].strip())

            if doc_date.year == current_year and doc_date.month == current_month:
                period = 'this_month'
            elif doc_date.year == last_month_year and doc_date.month == last_month:
                period = 'last_month'
            else:
                continue
            stcrd_record.append([stkcod, trnqty, period])

        # รับเข้า
        if docnum_prefix in ['HI', 'RI', 'RR', 'HP', 'PI', 'JI', 'LL']:
            if stkcod not in earliest_receive_date or doc_date < earliest_receive_date[stkcod][0]:
                earliest_receive_date[stkcod] = (doc_date, str(record.DOCNUM)[0:2].strip())

            stcrd_record.append([stkcod, trnqty, 'bought_in'])

        # ปรับปรุงเอกสารปรับยอด
        if docnum_prefix in ['JU', 'TK']:
            if trnqty > 0:
                stcrd_record.append([stkcod, trnqty, 'bought_in'])
            else:
                if doc_date.year == current_year and doc_date.month == current_month:
                    stcrd_record.append([stkcod, abs(trnqty), 'this_month'])
                elif doc_date.year == last_month_year and doc_date.month == last_month:
                    stcrd_record.append([stkcod, abs(trnqty), 'last_month'])

    table.close()

    df = pd.DataFrame(stcrd_record, columns=['STKCOD', 'TRNQTY', 'PERIOD'])
    df_withdraw = pd.DataFrame(
        [(stk, val[0], val[1]) for stk, val in latest_withdraw_date.items()],
        columns=['STKCOD', 'latest_withdraw_date', 'docnum_latest_export']
    )

    df_receive = pd.DataFrame(
        [(stk, val[0], val[1]) for stk, val in earliest_receive_date.items()],
        columns=['STKCOD', 'earliest_receive_date', 'docnum_earliest_import']
    )

    # เติม LPURDAT จาก df_purchase_date เฉพาะกรณีที่ไม่มีใน STCRD
    df_purchase_date['STKCOD'] = df_purchase_date['STKCOD'].astype(str)
    df_purchase_fill = df_purchase_date[~df_purchase_date['STKCOD'].isin(df_receive['STKCOD'])]
    df_purchase_fill = df_purchase_fill[['STKCOD', 'LPURDAT']].rename(columns={'LPURDAT': 'earliest_receive_date'})
    df_purchase_fill['earliest_receive_date'] = pd.to_datetime(df_purchase_fill['earliest_receive_date'],
                                                               errors='coerce')
    df_receive = pd.concat([df_receive, df_purchase_fill], ignore_index=True)

    # แก้ไขตรงนี้ — เพิ่มแถวว่างเข้า df ถ้า STKCOD ไม่ถูกบันทึกไว้ใน stcrd_record
    missing_stkcods = set(df_receive['STKCOD'].unique()) - set(df['STKCOD'].unique())
    if missing_stkcods:
        df = pd.concat([df, pd.DataFrame([{
            'STKCOD': stk,
            'TRNQTY': 0,
            'PERIOD': None
        } for stk in missing_stkcods])], ignore_index=True)

    # ทำการ merge
    df['STKCOD'] = df['STKCOD'].astype(str)
    df_withdraw['STKCOD'] = df_withdraw['STKCOD'].astype(str)
    df_receive['STKCOD'] = df_receive['STKCOD'].astype(str)

    df = df.merge(df_withdraw, on='STKCOD', how='left')
    df = df.merge(df_receive, on='STKCOD', how='left')
    df.sort_values(by=['STKCOD', 'PERIOD'], inplace=True)

    print('STCRD : Total : ' + str(time.time() - ts) + ' sec.')
    return df


def get_earliest_import_from_mysql():
    try:
        with dbCenter() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT stkcod, earliest_date_import, docnum AS docnum_earliest_import, source_file
                    FROM dashboard_product_header_date_import
                """)
                result = cursor.fetchall()
                return pd.DataFrame(result)
    except Exception as e:
        print(f"❌ ERROR: get_earliest_import_from_mysql: {e}")
        return pd.DataFrame()


def module_stmas(dbf_file_path_stmas):
    print('STMAS : Processing...')
    ts = time.time()

    table = dbf.Table(filename=dbf_file_path_stmas, default_data_types=dict(C=dbf.Char, D=dbf.Date), codepage='cp874')
    table.open(mode=dbf.READ_ONLY)

    data = table
    stmas_record = []
    for record in data:
        if dbf.is_deleted(record):
            continue
        try:
            lsel_dat = str(record.LSELDAT) if record.LSELDAT else None
        except Exception:
            lsel_dat = None

        try:
            lpur_dat = str(record.LPURDAT) if record.LPURDAT else None
        except Exception:
            lpur_dat = None
        stmas_record.append(
            [str(record.STKCOD), float(record.TOTBAL), float(record.UNITPR), float(record.TOTVAL), lsel_dat, lpur_dat])

    table.close()

    df = pd.DataFrame(stmas_record)
    df = df.set_axis(['STKCOD', 'TOTBAL', 'UNITPR', 'TOTVAL', 'LSELDAT', 'LPURDAT'], axis=1)
    df.sort_values(by=['STKCOD', 'LSELDAT'], inplace=True)
    # print(df)

    df2 = df.copy()
    df2.sort_values(by=['STKCOD', 'LPURDAT'], inplace=True)

    print('STMAS : Total : ' + str(time.time() - ts) + ' sec.')
    return df, df2


def calculate_withdrawn_and_bought_in_values(df_stcrd, df_stkcod):
    print('Calculate : Processing...')
    ts = time.time()

    # กรองเฉพาะข้อมูลที่อยู่ใน df_stkcod
    filtered_df = df_stcrd[df_stcrd['STKCOD'].isin(df_stkcod['STKCOD'])]

    if not filtered_df.empty:
        # คำนวณจำนวนสินค้าที่ถูกเบิกออก
        withdrawn_summary = (
            filtered_df[filtered_df['PERIOD'].isin(['this_month', 'last_month'])]
            .groupby(['STKCOD', 'PERIOD'])['TRNQTY']
            .sum()
            .unstack(fill_value=0)
            .reset_index()
        )

        # print("Withdrawn Summary Columns:", withdrawn_summary.columns)

        # **แก้ไขชื่อคอลัมน์ให้อยู่ในรูปแบบที่คาดหวัง**
        column_mapping = {}
        if 'this_month' in withdrawn_summary.columns:
            column_mapping['this_month'] = 'withdrawn_this_month'
        if 'last_month' in withdrawn_summary.columns:
            column_mapping['last_month'] = 'withdrawn_last_month'

        withdrawn_summary = withdrawn_summary.rename(columns=column_mapping)

        # ใช้ `.reindex()` เพื่อให้มีคอลัมน์ที่ต้องการเสมอ
        withdrawn_summary = withdrawn_summary.reindex(
            columns=['STKCOD', 'withdrawn_last_month', 'withdrawn_this_month'], fill_value=0)

        # คำนวณจำนวนสินค้าที่รับเข้า
        bought_in_summary = (
            filtered_df[filtered_df['PERIOD'] == 'bought_in']
            .groupby('STKCOD')['TRNQTY']
            .sum()
            .reset_index()
        )

        bought_in_summary.columns = ['STKCOD', 'total_bought_in_qty']

        # รวมข้อมูลของ withdrawn_summary และ bought_in_summary
        summary = pd.merge(withdrawn_summary, bought_in_summary, on='STKCOD', how='outer').fillna(0)

    else:
        summary = pd.DataFrame(
            columns=['STKCOD', 'withdrawn_last_month', 'withdrawn_this_month', 'total_bought_in_qty'])

    print('Calculate : Total : ' + str(time.time() - ts) + ' sec.')
    return summary


def export_source_file_report(df, output_path='source_file_report.xlsx'):
    try:
        export_df = df[['stkcod', 'stkdes', 'earliest_date_import', 'docnum_earliest_import', 'source_file']].copy()
        export_df.to_excel(output_path, index=False)
        print(f"📤 Export เสร็จเรียบร้อย: {output_path}")
    except Exception as e:
        print(f"❌ Export ล้มเหลว: {e}")


def insert_product_data(df):
    print('Insert MySQL : Processing...')
    ts = time.time()
    try:
        ############################ INSERT MYSQL ############################
        # ชื่อตาราง
        table_name = 'dashboard_product_header'

        # เชื่อมต่อฐานข้อมูล MySQL
        engine = create_engine(
            "mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(user=db_username, pw=db_password,
                                                                             host=db_host, db=db_database))

        # ลบข้อมูลเดิมในตาราง
        with engine.connect() as connection:
            connection.execute(text(f"DELETE FROM {table_name}"))
            # รีเซ็ตค่า AUTOINCREMENT
            connection.execute(text(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1"))

        # กำหนด timestamp และ คอลัมน์ที่ต้องการ
        timestamp = pd.Timestamp.now()
        df.loc[:, 'created_at'] = timestamp
        df.loc[:, 'updated_at'] = timestamp
        df = df[[
            'stkcod', 'stkdes', 'barcode', 'category', 'group', 'unit',
            'total_bought_in_qty', 'remaining_amount', 'cost', 'residual_value',
            'withdrawn_last_month', 'withdrawn_this_month',
            'latest_date_export', 'earliest_date_import',
            'docnum_latest_export', 'docnum_earliest_import',
            'owner_emp_id', 'created_at', 'updated_at'
        ]]
        # print("df_insert_product_data", df[df['stkcod'] == "PF -FP-U015C0LPP0"][['stkcod', 'earliest_receive_date']])
        # Reset index
        df.reset_index()

        # เปลี่ยนชื่อคอลัมน์
        df = df.set_axis([
            'stkcod', 'stkdes', 'barcod', 'category', 'group', 'unit', 'total_bought_in_amount',
            'remaining_amount', 'cost', 'residual_value',
            'withdrawn_last_month', 'withdrawn_this_month',
            'latest_date_export', 'earliest_date_import',
            'doc_out', 'doc_in',
            'owner', 'created_at', 'updated_at'
        ], axis=1)
        # เขียนข้อมูลใหม่ลงในตาราง
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=1000)

        print('Insert MySQL : Total : ' + str(time.time() - ts) + ' sec.')

    except Exception as e:
        print(f"Error: {e}")


def task():
    total_ts = time.time()

    tb_product = tbProductData()
    df_product = pd.DataFrame(tb_product)
    df_stkcod = df_product[['stkcod']]
    # Rename column
    df_stkcod.columns = ['STKCOD']

    df_stmas, df_purchase_date = module_stmas(dbf_file_path_stmas)
    df_stcrd = module_stcrd(dbf_file_path_stcrd, df_purchase_date)
    # print("ตรวจสอบข้อมูลหลัง module_stcrd")
    # print(df_stcrd[df_stcrd['STKCOD'] == "PF -FP-U015C0LPP0"][
    #           ['STKCOD', 'earliest_receive_date']])
    df_receive_multi_year = get_earliest_import_from_mysql()

    print("📦 สรุปจำนวน STKCOD ที่เจอ earliest_receive_date จากแต่ละไฟล์:")
    print(df_receive_multi_year['source_file'].value_counts())

    summary = calculate_withdrawn_and_bought_in_values(df_stcrd, df_stkcod)

    print('Filter : Processing...')
    ts = time.time()

    # สร้างแมสก์เพื่อตรวจสอบว่า STKCOD อยู่ใน df_stkcod หรือไม่
    mask1 = df_stmas['STKCOD'].isin(df_stkcod['STKCOD'])

    # เพิ่มคอลัมน์ใหม่โดยใช้ np.where
    df_stmas['remaining_amount'] = np.where(mask1, df_stmas['TOTBAL'], 0)
    df_stmas['cost'] = np.where(mask1, df_stmas['UNITPR'], 0)
    df_stmas['residual_value'] = np.where(mask1, df_stmas['TOTVAL'], 0)

    # สร้างแมสก์เพื่อตรวจสอบว่า STKCOD อยู่ใน df_stkcod หรือไม่
    mask2 = summary['STKCOD'].isin(df_stkcod['STKCOD'])

    # เปลี่ยนค่าในคอลัมน์โดยใช้ np.where
    summary['withdrawn_last_month'] = np.where(mask2, summary['withdrawn_last_month'], 0)
    summary['withdrawn_this_month'] = np.where(mask2, summary['withdrawn_this_month'], 0)
    summary['total_bought_in_qty'] = np.where(mask2, summary['total_bought_in_qty'], 0)

    df = pd.merge(
        df_product,
        df_stmas[['STKCOD', 'remaining_amount', 'cost', 'residual_value', 'LPURDAT']],
        left_on='stkcod',
        right_on='STKCOD',
        how='left'
    )

    df = pd.merge(df, summary[['STKCOD', 'withdrawn_last_month', 'withdrawn_this_month', 'total_bought_in_qty']],
                  left_on='stkcod', right_on='STKCOD', how='left')

    # print("ตรวจสอบข้อมูลก่อน groupby")
    # print(df_stcrd[df_stcrd['STKCOD'] == "PF -FP-U015C0LPP0"][
    #           ['STKCOD', 'earliest_receive_date']])

    df_stcrd_grouped = df_stcrd.groupby('STKCOD').agg({
        'latest_withdraw_date': 'max',
        'docnum_latest_export': 'last'
    }).reset_index().rename(columns={
        'latest_withdraw_date': 'latest_date_export'
    })

    if 'STKCOD' not in df_receive_multi_year.columns:
        if 'stkcod' in df_receive_multi_year.columns:
            df_receive_multi_year.rename(columns={'stkcod': 'STKCOD'}, inplace=True)
        else:
            print("🛑 ERROR: df_receive_multi_year ไม่มี STKCOD หรือ stkcod")
            print("Columns:", df_receive_multi_year.columns)
            return

    # 👇 เพิ่มบรรทัดนี้ก่อน merge
    df_receive_multi_year.rename(columns={
        'STKCOD': 'stkcod',
        'docnum': 'docnum_earliest_import'
    }, inplace=True)

    # print("df columns:", df.columns)
    # print("df_receive_multi_year columns:", df_receive_multi_year.columns)

    # แล้ว merge ได้เลย
    df = pd.merge(df, df_receive_multi_year, on='stkcod', how='left')

    df = pd.merge(df, df_stcrd_grouped, left_on='stkcod', right_on='STKCOD', how='left')

    df['earliest_date_import'] = df['earliest_date_import'].combine_first(df['LPURDAT'])

    # แปลง NaN เป็น 0
    columns_to_fill = ['remaining_amount', 'cost', 'residual_value', 'withdrawn_last_month', 'withdrawn_this_month',
                       'total_bought_in_qty']
    df[columns_to_fill] = df[columns_to_fill].fillna(0)

    print('Filter : Total : ' + str(time.time() - ts) + ' sec.')
    export_source_file_report(df)

    insert_product_data(df)

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

