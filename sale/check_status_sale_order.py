import xmlrpc.client
from datetime import datetime, timedelta
import time
import pandas as pd

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT26122023'
username = '660100'
password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def search():
    where = [('name', '=', "SO6702000007")]
    datas = models.execute_kw(db, uid, password, "sale.order", 'search_read', [where], {'fields': ['id', 'name', 'state']})
    for data in datas:
        print(data)


def update_state():
    where = [('name', '=', "SO6702000007")]
    datas = models.execute_kw(db, uid, password, "sale.order", 'search_read', [where], {'fields': ['id', 'name', 'state']})
    for data in datas:
        print(data)
        for so in datas:
            so_id = so['id']
            updated_data = {'state': 'draft'}
            result = models.execute_kw(db, uid, password, 'sale.order', 'write', [[so_id], updated_data])

            if result:
                print(f"Status for SO '{so_id}' updated successfully to {updated_data['state']}")
            else:
                print(f"Failed to update priority for product '{so_id}'.")


# search()
update_state()
