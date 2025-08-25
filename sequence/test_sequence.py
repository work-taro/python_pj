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
# db = 'tarotest'
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


def search():
    table_sequence = "ir.sequence"
    where_sequence = [("date_range_ids", "!=", False) and ("name", "like", "Advance")]
    data_sequence = models.execute_kw(db, uid, password, table_sequence, 'search_read', [where_sequence], {'fields': ['id', 'name']})
    for data in data_sequence:
        # print(data)
        print(data["id"])
        table_date_range = 'ir.sequence.date_range'
        # print(val)

        data_for_create = [
            {
                'sequence_id': data['id'],
                'date_from': "2025-01-01",
                'date_to': "2025-01-31",
            },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-02-01",
            #     'date_to': "2025-02-28",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-03-01",
            #     'date_to': "2025-03-31",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-04-01",
            #     'date_to': "2025-04-30",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-05-01",
            #     'date_to': "2025-05-31",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-06-01",
            #     'date_to': "2025-06-30",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-07-01",
            #     'date_to': "2025-07-31",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-08-01",
            #     'date_to': "2025-08-31",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-09-01",
            #     'date_to': "2025-09-30",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-10-01",
            #     'date_to': "2025-10-31",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-11-01",
            #     'date_to': "2025-11-30",
            # },
            # {
            #     'sequence_id': data['id'],
            #     'date_from': "2025-12-01",
            #     'date_to': "2025-12-31",
            # },
        ]

        create_date_range = models.execute_kw(db, uid, password, table_date_range, 'create', [data_for_create])

        # where_date_range = [("sequence_id", "=", data['id'])]
        # data_date_range = models.execute_kw(db, uid, password, table_date_range, 'search_read', [where_date_range],
        #                                     {'fields': ['id', 'date_from', 'date_to', 'sequence_id', 'display_name']})
        # for data_date in data_date_range:
        #     print(data_date)


search()
