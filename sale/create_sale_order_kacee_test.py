import xmlrpc.client
import pandas as pd
import pymysql
import re
from datetime import datetime
import datetime


# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT26122023'
username = '660100'
password = 'Kacee2023'

# EV
# url = 'http://192.168.9.101:8069/'
# db = 'ev_uat02'
# username = '660100'
# password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)

time_now = datetime.datetime.now()
t = time_now.strftime("%Y-%m-%d %H:%M:%S")
d = time_now.strftime("%Y-%m-%d")
print(t)
formatted_time = str(t)
formatted_delivery = str(d)

one_day = datetime.timedelta(days=1)
new_time = time_now + one_day
time_tomorrow = str(new_time)

# print("วันที่ปัจจุบัน:", time_now.strftime("%Y-%m-%d %H:%M:%S"))
# print("วันที่ +1 วัน:", new_time.strftime("%Y-%m-%d %H:%M:%S"))


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


# READ uom
def search_read_invoice_address():
    condition = [('name', '=', "168 ผ้าม่าน")]
    users = models.execute_kw(db, uid, password,
                              'res.partner', 'search_read',
                              [condition], {})

    for user in users:
        print(user)
        print(user[id])


def search_read_sale_order():
    condition = [('name', '=', "SO6701000269")]
    users = models.execute_kw(db, uid, password,
                              'sale.order', 'search_read',
                              [condition], {'fields': []})

    for user in users:
        print(user)
        print(user['order_line'])
        i = user['order_line']
        search_read_sale_order_line(i)


def search_read_sale_order_line(i):
    condition = [('id', '=', i[0])]
    users = models.execute_kw(db, uid, password,
                              'sale.order.line', 'search_read',
                              [condition], {'fields': []})

    for user in users:
        print(user)


def search_read_product_variants():
    name = "มู่ลี PVC604BLUEกว้าง 1.5 ม."
    name_without_space = name.replace(" ", " ")
    condition = [('name', '=', name_without_space)]
    users = models.execute_kw(db, uid, password,
                              'product.product', 'search_read',
                              [condition], {'fields': []})

    for user in users:
        print(user)


def find_partner_id():
    name_of_partner_id = "38 โฮม เดคคอร์"
    name_of_partner_id_without_space = name_of_partner_id.replace(" ", " ")
    condition = [('name', '=', name_of_partner_id_without_space)]
    users_partner_id = models.execute_kw(db, uid, password,
                                         'res.partner', 'search_read',
                                         [condition], {'fields': []})
    for user in users_partner_id:
        print(user)


def find_user_id():
    name_of_user_id = "วิสุทธิ์ คุปต์สันติ"
    # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
    condition = [('name', '=', name_of_user_id)]
    users_id = models.execute_kw(db, uid, password,
                                         'res.users', 'search_read',
                                         [condition], {'fields': []})
    for user in users_id:
        print(user)


def find_pricelist_id():
    name_of_pricelist_id = "Public Pricelist"
    # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
    condition = [('name', '=', name_of_pricelist_id)]
    pricelist_id = models.execute_kw(db, uid, password,
                                         'product.pricelist', 'search_read',
                                         [condition], {'fields': []})
    for user in pricelist_id:
        print(user)


def find_payment_check():
    name_of_payment_method = "เงินสด"
    # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
    condition = [('name', '=', name_of_payment_method)]
    payment_id = models.execute_kw(db, uid, password,
                                     'pricelist.payment.method', 'search_read',
                                     [condition], {'fields': []})
    for user in payment_id:
        print(user)


def find_sale_order_type():
    name_of_order_type = "Sale Vat"
    # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
    condition = [('name', '=', name_of_order_type)]
    order_type_id = models.execute_kw(db, uid, password,
                                   'sale.order.type', 'search_read',
                                   [condition], {'fields': []})
    for user in order_type_id:
        print(user)


# CREATE DATA
def create_data():
    name_of_partner_id = "TIK TOK SHOP"
    # name_of_partner_id_without_space = name_of_partner_id.replace(" ", " ")
    condition = [('name', '=', name_of_partner_id)]
    users_partner_id = models.execute_kw(db, uid, password,
                                         'res.partner', 'search_read',
                                         [condition], {'fields': []})
    for partner in users_partner_id:
        print(partner)
        print(partner['id'])

        name_of_user_id = "วิสุทธิ์ คุปต์สันติ"
        # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
        condition = [('name', '=', name_of_user_id)]
        users_id = models.execute_kw(db, uid, password,
                                     'res.users', 'search_read',
                                     [condition], {'fields': []})
        for user in users_id:
            # print(user)
            # print(user['id'])

            name_of_pricelist_id = "Public Pricelist"
            # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
            condition = [('name', '=', name_of_pricelist_id)]
            pricelist_id = models.execute_kw(db, uid, password,
                                             'product.pricelist', 'search_read',
                                             [condition], {'fields': []})
            for pricelist in pricelist_id:
                # print(pricelist)
                # print(pricelist['id'])

                name_of_payment_method = "เงินสด"
                # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
                condition = [('name', '=', name_of_payment_method)]
                payment_id = models.execute_kw(db, uid, password,
                                               'pricelist.payment.method', 'search_read',
                                               [condition], {'fields': []})
                for payment in payment_id:
                    # print(payment)
                    # print(payment['id'])

                    name_of_order_type = "Sale Vat"
                    # name_of_user_id_without_space = name_of_user_id.replace(" ", " ")
                    condition = [('name', '=', name_of_order_type)]
                    order_type_id = models.execute_kw(db, uid, password,
                                                      'sale.order.type', 'search_read',
                                                      [condition], {'fields': []})
                    for order_type in order_type_id:
                        # print(order_type)
                        # print(order_type['id'])

                        # create sale_order
                        new_data = {
                            'partner_id': partner['id'],
                            # 'partner_invoice_id': 1,
                            # 'partner_shipping_id': 1,
                            'user_id': user['id'],
                            'pricelist_id': pricelist['id'],
                            'payment_check': payment['id'],
                            'date_order': formatted_time,
                            'delivery_date': formatted_delivery,
                            'type_id': order_type['id'],
                            # 'order_line': [[order_line]],
                            'note': "TIK TOK SHOP - 001",

                        }
                        # print(new_data)
                        new_id = models.execute_kw(db, uid, password, 'sale.order', 'create', [new_data])
                        if new_id:
                            print("Sale Order created with ID:", new_id)
                        else:
                            print("Failed to create Sale Order")

                        # add product in order_line
                        # name_of_product = "แม็กกี้ ซอสปรุงอาหาร สูตรผัดกลมกล่อม  500 มล."
                        # name_of_product_without_space = name_of_product.replace(" ", " ")
                        name_of_product = [
                            "แม็กกี้ ซอสปรุงอาหาร สูตรผัดกลมกล่อม  500 มล.",
                            # "ซอสหอยนางรม สูตรดั้งเดิม ตราเด็กสมบูรณ์  800 ก."
                        ]
                        name_of_product_without_space = []

                        # ลูปผ่านชื่อสินค้าแต่ละตัวในลิสต์
                        for product in name_of_product:
                            # แทนที่ spacebar ด้วย non-breaking space และเพิ่มลงในลิสต์ใหม่
                            product_without_space = product.replace(" ", "\u00A0")
                            name_of_product_without_space.append(product_without_space)

                            condition = [('name', '=', name_of_product_without_space)]
                            product_variants = models.execute_kw(db, uid, password,
                                                      'product.product', 'search_read',
                                                      [condition], {'fields': []})

                            for p in product_variants:
                                print(p['id'])

                                order_line = {
                                    'order_id': new_id,
                                    'product_id': p['id'],
                                    'product_uom_qty': 5,
                                }
                                #
                                # product = [
                                #     {
                                #         'order_id': new_so_id,
                                #         'product_id': 217721,
                                #         'name': "test",
                                #         'product_uom_qty': 1,
                                #         'price_unit': 200,
                                #     },
                                #     {
                                #         'order_id': new_so_id,
                                #         'product_id': 251616,
                                #         'name': "test2",
                                #         'product_uom_qty': 1,
                                #         'price_unit': 120,
                                #     }
                                # ]
                                new_id_line = models.execute_kw(db, uid, password, 'sale.order.line', 'create', [order_line])
                                print(new_id_line)
                                if new_id_line:
                                    print("Sale Order Line created with ID:", new_id_line)
                                else:
                                    print("Failed to create Sale Order Line")

                                # find so number
                                condition = [('id', '=', new_id)]
                                so_info = models.execute_kw(db, uid, password,
                                                                 'sale.order', 'search_read',
                                                                 [condition], {'fields': []})
                                for i in so_info:
                                    print("so_info: ", i['name'])


# search_read_partner()
# search_read_pricelist()
# search_read_payment()
# search_read_sale_order_type()
# search_read_uom()
# search_read_invoice_address()
# search_read_sale_order()
# search_read_sale_order_line()
# find_partner_id()
# find_user_id()
# find_pricelist_id()
# find_payment_check()
# search_read_product_variants()
create_data()
