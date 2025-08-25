import xmlrpc.client
import pandas as pd
import pymysql
import re
from datetime import datetime
import datetime


# KC
# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT26122023'
# username = '660100'
# password = 'Kacee2023'

# EV
url = 'http://192.168.9.101:8069/'
db = 'ev_uat02'
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

time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(time_now)
formatted_time = str(time_now)


# READ
def search_read_partner():
    condition = [('ref', '=', "07-1-117")]
    users = models.execute_kw(db, uid, password,
                              'res.partner', 'search_read',
                              [condition], {'fields': ['ref', 'display_name']})

    for user in users:
        print(user)
        print(user['id'])


# READ pricelist
def search_read_pricelist():
    condition = [('name', '=', "เงินเครดิต1")]
    users = models.execute_kw(db, uid, password,
                              'product.pricelist', 'search_read',
                              [condition], {'fields': ['name']})

    for user in users:
        print(user)
        print(user['id'])
    print(formatted_time)


# READ payment
def search_read_payment():
    condition = [('name', '=', "เงินเครดิต")]
    users = models.execute_kw(db, uid, password,
                              'pricelist.payment.method', 'search_read',
                              [condition], {'fields': ['name']})

    for user in users:
        print(user)
        print(user['id'])


# READ sale_order_type
def search_read_sale_order_type():
    condition = [('name', '=', "เงินเครดิต")]
    users = models.execute_kw(db, uid, password,
                              'sale.order.type', 'search_read',
                              [[]], {'fields': ['name', 'warehouse_id', 'company_id']})

    for user in users:
        print(user)
        print(user['id'])


# READ sale_order
def search_read_sale_order():
    condition = [('name', '=', "เงินเครดิต")]
    users = models.execute_kw(db, uid, password,
                              'sale.order', 'search_read',
                              [[]], {'fields': ['name', 'state', 'date_order']})

    for user in users:
        print(user)
        print(type(user['date_order']))


# READ uom
def search_read_uom():
    condition = [('name', '=', "เงินเครดิต")]
    users = models.execute_kw(db, uid, password,
                              'uom.uom', 'search_read',
                              [[]], {'fields': ['name', 'category_id', 'uom_type']})

    for user in users:
        print(user)
        print(user['id'])


# CREATE DATA
def create_data():
    order_line = {
        'product_id': 11284,
        'name': "TEST",
        'product_uom_qty': 1,
        'product_uom': 76,
        'price_unit': 200,
    }
    new_data = {
        'partner_id': 119542,
        'partner_invoice_id': 1,
        'partner_shipping_id': 1,
        'user_id': 63,
        'pricelist_id': 4,
        'payment_check': 1,
        'date_order': formatted_time,
        'delivery_date': "2024-01-04",
        'type_id': 1,
        # 'order_line': order_line,

    }
    print(new_data)
    new_id = models.execute_kw(db, uid, password,
                                        'sale.order', 'create',
                                        [new_data])
    if new_id:
        print("Employee record created with ID:", new_id)
    else:
        print("Failed to create employee record")

    new_user_data = {
        'firstname': 'test',
        'lastname': 'create',
        'login': 'taro',
    }
    new_user_id = models.execute_kw(db, uid, password,
                                        'res.users', 'create',
                                        [new_user_data])
    if new_user_id:
        print("Employee record created with user:", new_user_id)
    else:
        print("Failed to create employee user record")


# search_read_partner()
# search_read_pricelist()
# search_read_payment()
# search_read_sale_order_type()
# search_read_sale_order()
# search_read_uom()
create_data()
