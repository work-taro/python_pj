import xmlrpc.client
from datetime import datetime, timedelta
import time
import pandas as pd
from xmlrpc import client

# KC
# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT_27022024'
# username = '660100'
# password = 'Kacee2023'

# My PC
url = 'http://localhost:8069'
db = 'tarotest_dropdown'
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


def favourite():
    # Kacee
    favourite_template = models.execute_kw(db, uid, password,
                                           'ir.filters', 'search_read',
                                           [[]], {
                                               'fields': []})
    for f in favourite_template:
        print(f)


def favourite_to_excel():
    # Kacee
    # favourite_template = models.execute_kw(db, uid, password,
    #                                        'ir.filters', 'search_read',
    #                                        [[]], {
    #                                            'fields': [
    #                                                'id', 'name', 'domain', 'context', 'sort', 'model_id',
    #                                                'is_default', 'action_id', 'active', 'display_name',
    #                                                'create_uid', 'create_date', 'write_uid', 'write_date',
    #                                                '__last_update', 'sequence', 'type', 'groupby_field', 'group_id', 'user_id'
    #                                            ]})
    # MyLocal
    favourite_template = models.execute_kw(db, uid, password,
                                           'ir.filters', 'search_read',
                                           [[]], {
                                               'fields': [
                                                   'id', 'name', 'domain', 'context', 'sort', 'model_id',
                                                   'is_default', 'action_id', 'active', 'display_name',
                                                   'create_uid', 'create_date', 'write_uid', 'write_date',
                                                   '__last_update', 'user_id', 'type'
                                               ]})
    df = pd.DataFrame(favourite_template)
    df['action_id'] = df['action_id'].apply(lambda x: x[1] if isinstance(x, list) else None)
    df['user_id'] = df['user_id'].apply(lambda x: x[1] if isinstance(x, list) else None)
    df['create_uid'] = df['create_uid'].apply(lambda x: x[0] if isinstance(x, list) else None)
    df['write_uid'] = df['write_uid'].apply(lambda x: x[0] if isinstance(x, list) else None)
    df = df.sort_values(by='id', ascending=True)

    df.to_excel("favourite_filters_mylocal.xlsx", index=False)

    print("Exported to favourite_filters.xlsx")


def favourite_group():
    favourite_group_template = models.execute_kw(db, uid, password,
                                                 'ir.filters', 'search_read',
                                                 [[]], {
                                                     'fields': []})
    for f in favourite_group_template:
        print(f)


def customer_search():
    condition = [('id', '=', 61431)]
    customer_template = models.execute_kw(
        db, uid, password,
        'res.partner', 'search_read',
        [condition], {'fields': []})
    if customer_template:
        for c in customer_template:
            print(c)
    else:
        print("Customer not found.")

# favourite()
# favourite_group()
favourite_to_excel()
# customer_search()


