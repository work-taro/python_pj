import xmlrpc.client

# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_DEV'
username = '660100'
password = 'Kacee2023'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def validate_delivery_test(delivery_name):
    condition = [('name', '=', delivery_name)]
    mrp_template = models.execute_kw(db, uid, password,
                                     'mrp.immediate.production', 'search_read',
                                     [], {
                                         'fields': []})
    for mrp in mrp_template:
        print(mrp)


# immediate
validate_delivery_test("WH/MO/00231")
