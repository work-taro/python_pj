# import pyodbc
import pandas as pd
from threading import Thread
from sqlalchemy import create_engine, text
from sqlalchemy.types import FLOAT, VARCHAR, DATE, TIMESTAMP, INTEGER
import re
import os
import time
import datetime
from urllib import parse

def dbSQLserver():
    # Connection details
    server = '192.168.2.20'  # IP ของเซิร์ฟเวอร์
    database = 'KACEE'  # ชื่อฐานข้อมูล
    username = 'KCapp'  # ชื่อผู้ใช้
    password = '12345'  # รหัสผ่าน

    # ใช้ ODBC Driver
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"
    engine = create_engine(connection_string)
    return engine

def dbEngine():
    # Center
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(user="kaceestockdeal_r", pw=parse.quote("6xPtebjhDRtL8Bo"),
                                                                         db="kacee_stock_dealer", host="127.0.0.1"))


    # Center DEV localhost
    # engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(user="kacee", pw="K_cee2022", db="kacee_stock_dealer", host="192.168.2.12"))

    # localhost
    # engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(user="root", pw="", db="kacee_stock_dealer", host="127.0.0.1", port="3306"))
    return engine

def remove_illegal_characters(value):
    """Remove illegal characters for Excel"""
    if isinstance(value, str):
        # Replace illegal characters with an empty string
        return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', value)

    return value


def remove_thai_characters(value):
    if isinstance(value, str):
        # Replace Thai characters with an empty string
        return re.sub(r'^[ก-๙]', '', value)
    return value


def clean_text_data(df, column):
    """ทำความสะอาดข้อมูลในคอลัมน์ที่ระบุ"""
    if column in df.columns:
        df[column] = df[column].astype(str).str.strip()
        df[column] = df[column].replace({'nan': None, 'None': None})
        df[column] = df[column].where(df[column] != '', None)
    return df

def clean_numeric_data(df, column):
    """เติมค่า Null ในคอลัมน์ตัวเลขด้วย 0"""
    if column in df.columns:
        df[column] = df[column].fillna(0)
    return df

def queryFirst():
    return """
        INSERT INTO products (
            ref_code,
            ref_value,
            ref_description,
            unit_uom,
            width,
            width_txt,
            width_uom,
            price_out,
            price,
            price_rate1,
            price_rate2,
            discount1,
            discount2,
            h_price,
            h_discount,
            sm_bal,
            sm_reserve,
            sm_scrab,
            composition,
            product_category_id,
            owner,
            active,
            flag_delete,
            created_at,
            updated_at
        )
        SELECT 
            pm.RefCode AS ref_code,
            pm.RefValue AS ref_value,
            pm.RefDescription AS ref_description,
            pm.UnitUOM AS unit_uom,
            pm.Width AS width,
            pm.WidthTxt AS width_txt,
            pm.WidthUOM AS width_uom,
            pm.PriceOut AS price_out,
            pm.Price AS price,
            pm.PriceRate1 AS price_rate1,
            pm.PriceRate2 AS price_rate2,
            pm.Discount1 AS discount1,
            pm.Discount2 AS discount2,
            pm.HPrice AS h_price,
            pm.HDiscount AS h_discount,
            pm.smBal AS sm_bal,
            pm.smReserve AS sm_reserve,
            pm.smScrab AS sm_scrab,
            pm.composition AS composition,
            COALESCE(pc.id, 8) AS product_category_id,
            pm.Owner AS owner,
            1 AS active,
            pm.FlagDelete AS flag_delete,
            NOW() AS created_at,
            NOW() AS updated_at
        FROM product_curtain pm
        LEFT JOIN product_match p_match ON pm.RefCode = p_match.ref_code
        LEFT JOIN product_categories pc ON p_match.category_name = pc.name
        WHERE pm.RefCode IS NOT NULL 
        AND pm.RefCode != ""
        ORDER BY pm.RefCode ASC
        ON DUPLICATE KEY UPDATE
            ref_value = VALUES(ref_value),
            ref_description = VALUES(ref_description),
            unit_uom = VALUES(unit_uom),
            width = VALUES(width),
            width_txt = VALUES(width_txt),
            width_uom = VALUES(width_uom),
            price_out = VALUES(price_out),
            price = VALUES(price),
            price_rate1 = VALUES(price_rate1),
            price_rate2 = VALUES(price_rate2),
            discount1 = VALUES(discount1),
            discount2 = VALUES(discount2),
            h_price = VALUES(h_price),
            h_discount = VALUES(h_discount),
            sm_bal = VALUES(sm_bal),
            sm_reserve = VALUES(sm_reserve),
            sm_scrab = VALUES(sm_scrab),
            composition = VALUES(composition),
            product_category_id = VALUES(product_category_id),
            owner = VALUES(owner),
            active = IF(VALUES(flag_delete) = 0, 0, VALUES(active)),
            flag_delete = VALUES(flag_delete),
            updated_at = VALUES(updated_at);
    """

def queryJob():
    return """
        INSERT INTO products (
            ref_code,
            ref_value,
            ref_description,
            unit_uom,
            width,
            width_txt,
            width_uom,
            price_out,
            price,
            price_rate1,
            price_rate2,
            discount1,
            discount2,
            h_price,
            h_discount,
            sm_bal,
            sm_reserve,
            sm_scrab,
            composition,
            product_category_id,
            owner,
            active,
            flag_delete,
            created_at,
            updated_at
        )
        SELECT 
            pm.RefCode AS ref_code,
            pm.RefValue AS ref_value,
            pm.RefDescription AS ref_description,
            pm.UnitUOM AS unit_uom,
            pm.Width AS width,
            pm.WidthTxt AS width_txt,
            pm.WidthUOM AS width_uom,
            pm.PriceOut AS price_out,
            pm.Price AS price,
            pm.PriceRate1 AS price_rate1,
            pm.PriceRate2 AS price_rate2,
            pm.Discount1 AS discount1,
            pm.Discount2 AS discount2,
            pm.HPrice AS h_price,
            pm.HDiscount AS h_discount,
            pm.smBal AS sm_bal,
            pm.smReserve AS sm_reserve,
            pm.smScrab AS sm_scrab,
            pm.composition AS composition,
            COALESCE(p.product_category_id, 8) AS product_category_id,
            pm.Owner AS owner,
            COALESCE(p.active, 1) AS active,
            pm.FlagDelete AS flag_delete,
            NOW() AS created_at,
            NOW() AS updated_at
        FROM product_curtain pm
        LEFT JOIN products p ON pm.RefCode = p.ref_code
        WHERE pm.RefCode IS NOT NULL 
        AND pm.RefCode != ""
        ORDER BY pm.RefCode ASC
        ON DUPLICATE KEY UPDATE
            ref_value = VALUES(ref_value),
            ref_description = VALUES(ref_description),
            unit_uom = VALUES(unit_uom),
            width = VALUES(width),
            width_txt = VALUES(width_txt),
            width_uom = VALUES(width_uom),
            price_out = VALUES(price_out),
            price = VALUES(price),
            price_rate1 = VALUES(price_rate1),
            price_rate2 = VALUES(price_rate2),
            discount1 = VALUES(discount1),
            discount2 = VALUES(discount2),
            h_price = VALUES(h_price),
            h_discount = VALUES(h_discount),
            sm_bal = VALUES(sm_bal),
            sm_reserve = VALUES(sm_reserve),
            sm_scrab = VALUES(sm_scrab),
            composition = VALUES(composition),
            flag_delete = VALUES(flag_delete),
            owner = VALUES(owner),
            active = IF(VALUES(flag_delete) = 0, 0, VALUES(active)),
            updated_at = VALUES(updated_at);
    """

def RunProgram():
    try:
        # Connect to SQL Server
        condb = dbSQLserver()
        query = "SELECT * FROM StockFabric"
        with condb.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
            # print(df)

            # Clean text special data
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].map(remove_illegal_characters)

            # Clean text thai data
            if 'RefCode' in df.columns:
                df['RefCode'] = df['RefCode'].map(remove_thai_characters)

                # Clean text " " to Null
                text_columns = ['RefValue', 'RefDescription', 'UnitUOM','Owner', 'Discount1', 'Discount2', 'composition', 'WidthTxt', 'WidthUOM']
                for col in text_columns:
                    df = clean_text_data(df, col)

                # Add Null = 0
                numeric_columns = ['FlagDelete', 'PriceOut', 'Price', 'PriceRate1', 'PriceRate2', 'HPrice', 'Width', 'smBal', 'smReserve', 'smScrab']
                for col in numeric_columns:
                    df = clean_numeric_data(df, col)

            # Add timestamp
            df['updated_at'] = datetime.datetime.now()

        # Connect to MySQL Center
        condb2 = dbEngine()
        with condb2.connect() as conn2:
            # Begin transaction พี่อํ๋น = product_curtain
            trans = conn2.begin()
            try:
                print("Insert product_curtain : Start....")
                # Delete existing data
                query_delete = "DELETE FROM product_curtain"
                conn2.execute(text(query_delete))

                # Write new data to MySQL
                df.to_sql('product_curtain', con=conn2, if_exists='append', index=False)
                print("Insert product_curtain : Successful.")

                # Commit transaction
                trans.commit()
            except Exception as e:
                trans.rollback()
                print("Error Insert product_ref", e)

            # Begin second transaction Center = products
            trans = conn2.begin()
            try:
                print("Insert products : Start....")

                # ใช้ SELECT COUNT(*) เพื่อตรวจสอบจำนวนข้อมูลในตาราง products
                query_check = "SELECT COUNT(*) AS count FROM products"
                result = conn2.execute(text(query_check)).fetchone()
                is_products_empty = result[0] == 0

                # หากตารางว่าง(count == 0) จะใช้ queryFirst()
                if is_products_empty:
                    print("Insert products process : queryFirst....")
                    query_upsert = queryFirst()

                # หากตารางมีข้อมูลอยู่แล้วจะใช้ queryJob()
                else:
                    print("Insert products process : queryJob.....")
                    query_upsert = queryJob()

                conn2.execute(text(query_upsert))
                print("Insert products : Successful.")

                # Commit transaction
                trans.commit()
            except Exception as e:
                trans.rollback()
                print("Error Insert products", e)

    except Exception as e:
        print("Error occurred:", e)

def quit_program():
    time.sleep(2)
    os._exit(0)


if __name__ == "__main__":
    print('=============== START PROGRAME =================')
    thread = Thread(target=RunProgram)
    thread.start()
    thread.join()
    print('================ END PROGRAME ==================')
    quit_program()
