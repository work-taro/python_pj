import xmlrpc.client

# กำหนดข้อมูลการเชื่อมต่อ
# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
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
    condition = [('name', '=', 'S00678')]
    sale_advance = models.execute_kw(
        db, uid, password, 'sale.order', 'search_read',
        [condition], {'fields': []}
    )
    for sa in sale_advance:
        print('Sale Order : ', sa)
        condition_so_line = [('id', '=', 1185)]
        sale_order_line = models.execute_kw(
            db, uid, password, 'sale.order.line', 'search_read',
            [condition_so_line], {'fields': []}
        )
        for l in sale_order_line:
            product_id = l['product_id']
            print('Sale Order Line : ', l)
            print('Product ID : ', product_id)

# 253001
read_immediate()
