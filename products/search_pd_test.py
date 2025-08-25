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
db = 'tarotest'
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
    condition = [
        '&',
        ('product_template_attribute_value_ids', 'in', [799162]),
        ('product_template_attribute_value_ids', 'in', [799093]),
        ('product_template_attribute_value_ids', 'in', [799626]),
        ('product_template_attribute_value_ids', 'in', [799628]),
        ('product_template_attribute_value_ids', 'in', [799631]),
        ('product_template_attribute_value_ids', 'in', [799649]),
        ('product_template_attribute_value_ids', 'in', [799655]),
        ('product_template_attribute_value_ids', 'in', [799675]),
        ('product_template_attribute_value_ids', 'in', [799676]),
        ('product_template_attribute_value_ids', 'in', [799682]),
        ('product_template_attribute_value_ids', 'in', [799686]),
    ]
    ids = models.execute_kw(db, uid, password,
                            'product.product', 'search',
                            [condition])

    if ids:
        users = models.execute_kw(db, uid, password,
                                  'product.product', 'read',
                                  [ids], {
                                      'fields': []
                                  })

        for user in users:
            print(user)


def search_attr():
    attribute_id = 252759
    attribute_ids = models.execute_kw(db, uid, password,
                                      'product.template', 'search_read',
                                      [[('id', '=', attribute_id)]],
                                      {'fields': ['id', 'name', 'categ_id', 'attribute_line_ids']})

    for a in attribute_ids:
        attribute_line_ids = a['attribute_line_ids']
        # print(attribute_line_ids)
        attribute_search = models.execute_kw(db, uid, password,
                                             'product.template.attribute.line', 'search_read',
                                             [[('id', '=', attribute_line_ids)]],
                                             {'fields': []})
        for at in attribute_search:
            print(at)
            attribute = at['attribute_id']
            attribute_value_id = at['value_ids']
            # print(attribute_value_id)
            # print(f"Attribute : {attribute[1]}")
            for atv in attribute_value_id:
                attribute_value_search = models.execute_kw(db, uid, password,
                                                     'product.attribute.value', 'search_read',
                                                     [[('id', '=', atv)]],
                                                     {'fields': []})
                # for atv_txt in attribute_value_search:
                    # print(atv_txt)


def search_account_move():
    condition = [('name', '=', 'OL/2024/07/0004')]
    ids = models.execute_kw(db, uid, password,
                            'account.move', 'search_read',
                            [condition])

    if ids:
        for user in ids:
            print(user)


search_account_move()
