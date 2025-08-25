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


def create_currency():
    # condition = [('name', '=', 'USD')]
    # currency_model = models.execute_kw(db, uid, password, 'res.currency', 'search_read',
    #                                    [], {'fields': []})
    # for cur in currency_model:
    #     print(cur)
    data = {
        'currency_id': 2,
        'name': '2024-05-25',
        'inverse_company_rate': '40.000',
    }
    currency_create = models.execute_kw(db, uid, password, 'res.currency.rate', 'create', [data])


create_currency()
