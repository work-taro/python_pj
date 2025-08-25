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
    name_so = 'S00804'
    condition = [('name', '=', name_so)]
    ids = models.execute_kw(db, uid, password,
                            'sale.order', 'search_read',
                            [condition], {'fields': []})
    for i in ids:
        sale_order_line_id = i['order_line']
        print(sale_order_line_id)
        for l in sale_order_line_id:
            print(l)
            so_line_condition = [('id', '=', l)]
            sale_order_line = models.execute_kw(db, uid, password,
                              'sale.order.line', 'search_read',
                              [so_line_condition], {'fields': []})
            for s in sale_order_line:
                print(s)

    # if ids:
    #     users = models.execute_kw(db, uid, password,
    #                               'product.product', 'read',
    #                               [ids], {
    #                                   'fields': []
    #                               })
    #
    #     for user in users:
    #         print(user)


search()
