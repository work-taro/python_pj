import xmlrpc.client
from datetime import datetime, timedelta
import time
import pandas as pd

# KC
# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT_27022024'
# username = '660100'
# password = 'Kacee2023'

# My PC
url = 'http://localhost:8069'
db = 'tarotest2'
username = 't'
password = '1'
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
    condition = [('name', '=', 'OL/2024/07/0005')]
    # condition = [['default_code', "!=", False], ['default_code', "=", "FURN_9666"]]
    users = models.execute_kw(db, uid, password,
                              'account.move', 'search_read',
                              [condition], {
                                  'fields': ['invoice_payments_widget']})

    for user in users:
        print(user['invoice_payments_widget'])


search()
