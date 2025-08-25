import xmlrpc.client
from datetime import datetime, timedelta
import time
import pandas as pd

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# My PC
# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'
# api key : 919f151fe2e22569f59a5fc6d356a097582efd8b

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def search():
    condition = [('qty_available', '=', False), ['default_code', "!=", False], ['default_code', "=", "8851932459563"]]
    # condition = [['default_code', "!=", False], ['default_code', "=", "FURN_9666"]]
    users = models.execute_kw(db, uid, password,
                              'product.template', 'search_read',
                              [[]], {
                                  'fields': ['id', 'default_code', 'barcode', 'name']})

    for user in users:
        print(user)
    if users:
        data = pd.DataFrame(users)
        data.to_excel('product_with_id.xlsx', index=False)
        print("Data has been written to product_with_id.xlsx")
    else:
        print("No data retrieved from XML-RPC")


search()
