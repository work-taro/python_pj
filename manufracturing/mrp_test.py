import xmlrpc.client
from datetime import datetime, timedelta
import time
import pandas as pd

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_DEV'
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


def search_mo():
    number = 0
    number_attr = 0
    mo_name = "WH/MO/00240"
    condition = [("name", "=", mo_name)]
    # condition = [['default_code', "!=", False], ['default_code', "=", "FURN_9666"]]
    mo_template = models.execute_kw(db, uid, password,
                              'mrp.production', 'search_read',
                              [condition], {
                                  'fields': []})
    for mo in mo_template:
        print("mo_all_field", mo)
        print("mo_product", mo['product_tmpl_id'])
        print("mo_product_id", mo['product_tmpl_id'][0])
        product_id = mo['product_tmpl_id'][0]
        condition_find_cate = [("id", "=", product_id)]
        # condition = [['default_code', "!=", False], ['default_code', "=", "FURN_9666"]]
        product_template = models.execute_kw(db, uid, password,
                                        'product.template', 'search_read',
                                        [condition_find_cate], {
                                            'fields': []})
        for product in product_template:
            print("product_all_field", product)
            print("product_cat", product['categ_id'])
            print("name_of_product_cat", product['categ_id'][1])


def search_stock():
    number = 0
    number_attr = 0
    product_name = "[6923492584206] เมย์เบลลีน มาสคาร่ากันน้ำไฮเปอร์เคิร์ล 9.2 มล.สีดำ"
    condition = [("product_id", "=", product_name),
                 ("location_id", "!=", 'Virtual Locations/Inventory adjustment')
                 ]
    stock_template = models.execute_kw(db, uid, password,
                              'stock.quant', 'search_read',
                              [condition], {
                                  'fields': []})
    for st in stock_template:
        print(st)


def create_onhand():
    product_cat = "สินค้าสำเร็จรูป"
    condition = [("categ_id", "=", product_cat)]
    product_template = models.execute_kw(db, uid, password,
                                       'product.template', 'search_read',
                                       [condition], {
                                           'fields': ['id', 'name', 'default_code', 'display_name', ]})
    for pd in product_template:
        print(pd)
        product_name = "[A27-10201] กล่องบน สีครีมC201"
        condition = [("product_id", "=", product_name)]
        stock_template = models.execute_kw(db, uid, password,
                                           'stock.quant', 'search_read',
                                           [condition], {
                                               'fields': []})


def create_stock_quant():
    location_name = "F4/Stock"
    location_id = models.execute_kw(db, uid, password,
                                    'stock.location', 'search',
                                    [[['complete_name', '=', location_name]]])

    if not location_id:
        raise ValueError("Location not found")

    # Define product and stock data
    product_name = "[6923492584206] เมย์เบลลีน มาสคาร่ากันน้ำไฮเปอร์เคิร์ล 9.2 มล.สีดำ"
    product_id = models.execute_kw(db, uid, password,
                                   'product.product', 'search',
                                   [[['display_name', '=', product_name]]])

    if not product_id:
        raise ValueError("Product not found")

    data = {
        'product_id': product_id[0],
        'location_id': location_id[0],
        'inventory_quantity': 10
    }

    # Create stock quant
    stock_template = models.execute_kw(db, uid, password,
                                       'stock.quant', 'create',
                                       [data])


# search_mo()
# search_stock()
# create_onhand()
create_stock_quant()
