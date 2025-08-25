import xmlrpc.client

# กำหนดข้อมูลการเชื่อมต่อ
# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_DEV'
username = '660100'
password = 'Kacee2023'

# สร้าง ServerProxy สำหรับเชื่อมต่อกับ Odoo
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def read_immediate():
    immediate_transfer = models.execute_kw(
        db, uid, password, 'stock.immediate.transfer', 'search_read',
        [], {'fields': []}
    )
    for im in immediate_transfer:
        print(im)


read_immediate()
