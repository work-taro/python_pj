import xmlrpc.client
import pandas as pd
import pymysql
import re
from datetime import datetime
import datetime


# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# EV
# url = 'http://192.168.9.101:8069/'
# db = 'ev_uat02'
# username = '660100'
# password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)

now = datetime.datetime.now()

# แปลงเป็น string ในรูปแบบของ timestamp ใน SQL
timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
print(timestamp_str)


def search_read():
    # ตั้งค่าการเชื่อมต่อกับฐานข้อมูลปลายทาง
    destination_dbname = 'odoo_kacee'
    destination_user = 'root'
    destination_password = ''
    destination_host = 'localhost'
    destination_port = 3306

    # เชื่อมต่อกับฐานข้อมูลปลายทาง
    conn = pymysql.connect(
        database=destination_dbname,
        user=destination_user,
        password=destination_password,
        host=destination_host,
        port=destination_port
    )
    cursor = conn.cursor()
    condition = [('default_code', '!=', "False")]
    users = models.execute_kw(db, uid, password,
                              'product.template', 'search_read',
                              [condition], {'fields': ['default_code', 'barcode', 'name', 'display_name', 'uom_id', 'list_price', 'categ_id']})
    # sql = "DELETE FROM ex_product_odoo"
    # cursor.execute(sql)
    #
    # alter = "ALTER TABLE ex_product_odoo AUTO_INCREMENT=1;"
    # cursor.execute(alter)

    # ล้างข้อมูลในตาราง ex_product_odoo ด้วย TRUNCATE แทนการใช้ DELETE
    truncate_sql = "TRUNCATE TABLE ex_product_odoo"
    cursor.execute(truncate_sql)

    # select = "SELECT * FROM ex_product_odoo"
    # cursor.execute(select)
    number = 1
    for user in users:
        try:
            # Extracting data
            barcode = user.get('barcode')
            name = user.get('name', "")
            display_name = user.get('display_name', "")

            default_code = user.get('default_code')
            if default_code != False:
                barcode = user.get('barcode')
                if barcode is False:
                    barcode = ""
                name = user.get('name')
                if name is False:
                    name = ""
                display_name = ""
                if display_name is False:
                    display_name = ""
                else:
                    display_name = user.get('display_name')
                if user.get('uom_id'):
                    uom_id = user.get('uom_id')[0]
                else:
                    uom_id = 0

                if user.get('list_price'):
                    list_price = user.get('list_price')
                else:
                    list_price = 0

                # print(user.get('list_price'), "listPrice")
                if user.get('categ_id') and isinstance(user['categ_id'], list) and len(user['categ_id']) >= 2:
                    categ_split = user['categ_id'][1].split("/ ")
                    user['categ_id'] = user['categ_id'][0]  # เก็บค่าหน้า "/" ไว้ใน categ_id
                    user['categ_id2'] = categ_split[-1]  # เก็บค่าหลัง "/" ไว้ใน categ_id2

                    # ถ้ามีคำก่อน "/" ใน list มากกว่า 1 คำ จะนำคำที่เหลือมาใส่ใน categ_id
                    if len(categ_split) > 1:
                        user['categ_id'] = categ_split[0]

                # Inserting data into ex_customer (adjust the columns and VALUES)
                insert_sql = """
                                    INSERT INTO ex_product_odoo (No, stkcod, barcod, names, stkdes, qucod, sellpr1, acccod, stkgrp, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                cursor.execute(insert_sql, (number, default_code, barcode, name, display_name, uom_id, list_price, user['categ_id'], user['categ_id2'], timestamp_str))
                number += 1
        except Exception as e:
            print(f"Error : {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Successfully for insert product")


# CREATE DATA
def create_data():
    new_product_id = {
        'name': 'testbytaro',
        'type': 'product',
        'uom_id': 1,
        'uom_po_id': 1,
    }
    new_product_id = models.execute_kw(db, uid, password,
                                        'product.template', 'create',
                                        [new_product_id])
    if new_product_id:
        print("Employee record created with ID:", new_product_id)
    else:
        print("Failed to create employee record")


search_read()
# create_data()

