import xmlrpc.client
import pandas as pd
import pymysql
import re


# KC
# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT26122023'
# username = '660100'
# password = 'Kacee2023'

# EV
# url = 'http://192.168.9.101:8069/'
# db = 'ev_uat02'
# username = '660100'
# password = 'Kacee2023'

# Localhost
url = 'http://localhost:8069'
db = 'test1'
username = 't'
password = '1'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


# UPDATE DATA
def update_data():
    condition = [('name', 'ilike', 'marc')]
    users = models.execute_kw(db, uid, password,
                              'res.users', 'search_read',
                              [[]], {'fields': ['id', 'firstname', 'lastname']})
    # for user in users:
    #     print(user)

    for user in users:
        user['firstname'], user['lastname'] = user['lastname'], user['firstname']

    for user in users:
        updated_data = {
            'firstname': user['firstname'],
            'lastname': user['lastname'],
        }
        result = models.execute_kw(db, uid, password,
                                   'res.users', 'write',
                                   [[user['id']], updated_data])

        if result:
            print(f"Records updated successfully for user ID {user['id']}")
        else:
            print(f"Failed to update records for user ID {user['id']}")


update_data()

