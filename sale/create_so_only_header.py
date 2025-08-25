import xmlrpc.client
import pandas as pd
import pymysql
import re
from datetime import datetime, timedelta
import time

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT26122023'
username = '660100'
password = 'Kacee2023'

# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT_27022024'
# username = '660100'
# password = 'Kacee2023'

# Localhost
# url = 'http://localhost:8069'
# db = 'test1'
# username = 't'
# password = '1'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


val = [{
    'customer_code': "3-03-002",
    'type_id': "Normal Order",
    'order_date': "10/03/2024 12:15:54",
    'delivery_date': "10/03/2024",
    'note': "240131AYYVAVMN",
    'product': [
        {
            'product_name': '8851907295882',
            'qty': 1,
        },
    ],
},
    # {
    #     'customer_code': "TIK TOK SHOP",
    #     'type_id': "Sale Vat",
    #     'order_date': "30/01/2024 14:33:59",
    #     'delivery_date': "30/01/2024",
    #     'note': "TIKTOK1",
    #     'product': [
    #         {
    #             'product_name': '8851989024448',
    #             'qty': 20,
    #         },
    #     ],
    # },
]


def search(val):
    table = 'res.partner'
    table2 = 'sale.order.type'
    table3 = 'stock.warehouse'

    # where = []
    # where = [('name_company', '=', cusnam)]

    # datas = models.execute_kw(db, uid, password, table, 'search_read', [where], {'fields': ['id',]})
    # cusid = datas[0]['id']
    # create(cusid)

    # print(val)
    for row in val:
        where = [('ref', '=', row['customer_code'])]
        datas = models.execute_kw(db, uid, password, table, 'search_read', [where], {'fields': ['id', ]})

        where2 = [('name', '=', row['type_id'])]
        datas2 = models.execute_kw(db, uid, password, table2, 'search_read', [where2], {'fields': ['id', 'warehouse_id']})

        cusid = datas[0]['id']
        typeId = datas2[0]['id']
        warehouse_id = datas2[0]['warehouse_id']
        wh_name = warehouse_id[1]
        # print(wh_name)

        where3 = [('name', '=', warehouse_id[1])]
        datas3 = models.execute_kw(db, uid, password, table3, 'search_read', [where3],
                                   {'fields': ['id', ]})
        wh_id = datas3[0]['id']
        # print(wh_id)

        # break

        create(row, cusid, typeId, wh_id)
        time.sleep(1)


def create(row, cusid, typeId, wh_id):
    _date = datetime.strptime(row['delivery_date'], "%d/%m/%Y").strftime("%Y-%m-%d")

    _datetime = datetime.strptime(row['order_date'], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    order_date = datetime.strptime(row['order_date'], "%d/%m/%Y %H:%M:%S") - timedelta(hours=7)
    od = order_date.strftime("%Y-%m-%d %H:%M:%S")
    print(_date, "delivery_date")
    print(od, "order_date")

    data = {
        'partner_id': cusid,
        'user_id': 2129,
        'date_order': od,
        'pricelist_id': 1,
        'delivery_date': _date,
        'note': row['note'],
        'type_id': typeId,
        'warehouse_id': wh_id,
    }

    so_id = models.execute_kw(db, uid, password, 'sale.order', 'create', [data])

    for i in row['product']:
        # print(i)
        product_name = i['product_name']
        qty_product = i['qty']
        # print(product_name)
        condition = [('default_code', '=', product_name)]
        product_variants = models.execute_kw(db, uid, password,
                                             'product.product', 'search_read',
                                             [condition], {'fields': []})
        for j in product_variants:
            # print(j['id'])
            product = [
                    {
                        'order_id': so_id,
                        'product_id': j['id'],
                        'product_uom_qty': qty_product,
                    },
                ]

            so_product_id = models.execute_kw(db, uid, password, 'sale.order.line', 'create', [product])
            # print("so_product_id", so_product_id[0])

            condition_find_so_number = [('id', '=', so_id)]
            so_info = models.execute_kw(db, uid, password,
                                        'sale.order', 'search_read',
                                        [condition_find_so_number], {'fields': []})
            for name in so_info:
                print(f"This order {row['note']} is so_number {name['name']}")


search(val)
