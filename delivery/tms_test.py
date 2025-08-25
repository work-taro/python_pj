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
    delivery_slip = "DN 202407 - 00008"
    condition = [('name', '=', delivery_slip)]
    # condition = [['default_code', "!=", False], ['default_code', "=", "FURN_9666"]]
    users = models.execute_kw(db, uid, password,
                              'distribition.delivery.note', 'search_read',
                              [condition], {
                                  'fields': []})

    for user in users:
        print(user)
        id_delivery_slip = user['id']
        name_delivery_slip = user['name']
        transport_line_name = user['transport_line_id'][1]
        mode_delivery = user['company_round_id'][1]
        schedule_date = user['schedule_date']
        delivery_date = user['delivery_date']
        invoice_line_ids = user['invoice_line_ids'][0]

        print(f"delivery slip id {id_delivery_slip}")
        print(f"name {name_delivery_slip}")
        print(f"สายส่ง : {transport_line_name}")
        print(f"mode delivery : {mode_delivery}")
        print(f"schedule_date {schedule_date}")
        print(f"delivery_date {delivery_date}")
        print(f"invoice line id {invoice_line_ids}")

        condition_invoice_line = [('id', '=', invoice_line_ids)]
        distribition_invoice_line = models.execute_kw(db, uid, password,
                              'distribition.invoice.line', 'search_read',
                              [condition_invoice_line], {
                                  'fields': []})
        for l in distribition_invoice_line:
            print(l)
            invoice_id = l['invoice_id'][0]
            invoice_name = l['name']
            invoice_date = l['invoice_date']
            partner_name = l['partner_id'][1]
            so_number = l['sale_no']
            delivery_id = l['delivery_id'][0]
            delivery_name = l['delivery_id'][1]

            print(f"invoice id {invoice_id}")
            print(f"invoice name {invoice_name}")
            print(f"invoice date {invoice_date}")
            print(f"partner name {partner_name}")
            print(f"SO number {so_number}")
            print(f"delivery id {delivery_id}")
            print(f"delivery name {delivery_name}")


search()
