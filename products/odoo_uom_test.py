import xmlrpc.client
import pandas as pd
import pymysql
import re


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

cuscod = ''
prenam = ''
cusnam = ''
city = ''
state = ''
street = ''
zipcod = ''
tel = ''
slmcod = ''
credit_limit = 0
paytrm = 0

arr_prenam = [
    'บริษัท',
    '.ริษัท',
    '56',
    'Co.,ltd.',
    'Dr',
    'Dr.',
    'K.',
    'Khun',
    'Miss',
    'MR.',
    'Mrs.',
    'Ms.',
    'Shop',
    'คณ',
    'คณะบุคคล',
    'คลีนิค',
    'คุณ',
    'คุณ.',
    'คุณว่าที่ร.ต.',
    'คุณหมอ',
    'คุณะ',
    'คุร',
    'งาน',
    'จ.สอ.',
    'จ.อ.',
    'จก.',
    'จสอ.',
    'จ่าสิบเอก',
    'ด.ต.',
    'ดร.',
    'ตัวอย่าง',
    'ทพญ.',
    'ธนาคาร',
    'น.พ.',
    'น.ส.',
    'นพ.',
    'นางสาว',
    'นาย',
    'นาวาอากาศโท',
    'นิติบุคคล',
    'บ.',
    'บจ.',
    'บจก.',
    'บมจ.',
    'บมจ.ธนาคาร',
    'บริษัท',
    'บริษํท',
    'ผกก.',
    'ผศ.',
    'ผศ.ดร.',
    'ผส.',
    'ผอ.',
    'ผอ.ดร.',
    'พ.ต.ท',
    'พ.ต.ท.',
    'พ.ต.อ',
    'พ.ต.อ.',
    'พ.ท.หญิง',
    'พ.อ',
    'พ.อ.',
    'พ.อ.อ.',
    'พญ.',
    'พล.ต.',
    'พล.ต.ต',
    'พล.ต.ต.',
    'พล.ตรี',
    'พลเอก',
    'พันตรี',
    'พันเอก',
    'พันเอก.ญ.',
    'พันโท',
    'มรว.',
    'มหาวิทยาลัย',
    'มูลนิธิสหพันธ์',
    'ร.ด.',
    'ร.ต.',
    'ร.ต.ท',
    'ร.ต.อ นพ.',
    'ร.ต.อ.',
    'รอ.ตร',
    'รัาน',
    'ร้าน',
    'ร้าน.',
    'ร้านที่นอน',
    'ร้านห้องเสื้อ',
    'วัด',
    'ว่าที่ร้อยตรี',
    'ศาตราจารย์ ดร.',
    'ส.ต.ท.',
    'ส.ท.',
    'ส.อ.',
    'ส.อ.หญิง',
    'สถาบัน',
    'สถาบันวิจัย',
    'สนง',
    'สนง.',
    'สหกรณ์',
    'สำนักงาน',
    'สำนักงานพัฒนา',
    'ห.จ.ก.',
    'ห.ส.ม.',
    'หจก',
    'หจก.',
    'หสน.',
    'หสน.จำกัด',
    'หสม',
    'หสม.',
    'หหสม.',
    'ห้องเสื้อ',
    'ห้าง',
    'ห้างกันสาด',
    'ห้างผ้า',
    'ห้างหุ้นส่วน',
    'อาจารย์',
    'โรงงาน',
    'โรงพยาบาล',
    'โรงเรียน',
    'โรงแรม',
]
prefix_list = list(set(arr_prenam))

def extract_prefix(words, prefix_list):
    for prefix in prefix_list:
        if words.startswith(prefix):
            # Remove the prefix and return the prefix with the remaining words
            return prefix, words[len(prefix):].strip()
    # Return empty prefix if none found
    return '', words

def search_read():
    # customer
    # condition = [('customer', '=', 'true')]

    # vendor
    condition = [('customer', '=', 'false')]
    users = models.execute_kw(db, uid, password, 'uom.uom', 'search_read', [[]],
                              {'fields': [
                                  'id',
                                  'name',
                                  'category_id',
                                  'uom_type',
                              ]})

    # sql = "DELETE FROM ex_unit_odoo"
    # cursor.execute(sql)
    # alter = "ALTER TABLE ex_unit_odoo AUTO_INCREMENT=1;"
    # cursor.execute(alter)
    truncate_sql = "TRUNCATE TABLE ex_unit_odoo"
    cursor.execute(truncate_sql)

    i = 1
    for user in users:
        print(user)
        try:
            if user.get('id') != False:
                id = user.get('id')
            else:
                id = ''

            if user.get('name') != False:
                name = user.get('name').strip()
            else:
                name = ''
            if user.get('category_id') != False:
                category_id = user.get('category_id')[1]
            else:
                category_id = ''
            if user.get('uom_type') != False:
                uom_type = user.get('uom_type').strip()
            else:
                uom_type = ''


            # if user.get('display_name') != False:
            #     display_name = user.get('display_name')
            #     prenam, cusnam = extract_prefix(display_name, prefix_list)
            # else:
            #     prenam = cusnam = ''

            # Inserting data into ex_customer (adjust the columns and VALUES)
            insert_sql = """
                    INSERT INTO ex_unit_odoo (ID, UnitofMeasure, Category, Type)
                    VALUES (%s, %s, %s, %s)
                """
            cursor.execute(insert_sql, (id, name, category_id, uom_type))

        except Exception as e:
            print(e)
        i += 1

    conn.commit()
    cursor.close()
    conn.close()

    print("Insert Success is Row: ", i)

if __name__ == '__main__':
    search_read()