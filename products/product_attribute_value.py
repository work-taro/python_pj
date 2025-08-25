import xmlrpc.client

# connect to server
# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})

models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

condition = []

condition_taro = [("login", "=", "660100")]
users = models.execute_kw(db, uid, password, 'product.attribute.value', 'search_read', [[]],
                          {'fields': ['id', 'attribute_id', 'name']})
print(users)

# for user in users:
#     print(user)
