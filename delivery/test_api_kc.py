import xmlrpc.client

url = 'http://192.168.9.102:8069' #https://dev-kc.kaceebest.com/
db = 'dev_kc'
username = '660100'
password = 'Kacee2023'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)
