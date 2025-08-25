import xmlrpc.client

# connect to server
url = 'http://localhost:8069'
db = 'tarotest2'
username = 't'
password = '1'

# KC
# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT_27022024'
# username = '660100'
# password = 'Kacee2023'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

condition = []
condition_without_knc = ["&", ("login", "!=", "kncadm"), ("login", "!=", "distawat.p"), ("login", "!=", "kanthalida.w"),
                         ("login", "!=", "pure"),
                         ("login", "!=", "pattanapong.j"), ("login", "!=", "pitchayisa.c"), ("login", "!=", "warat.m")
                         ]

condition_taro = [("login", "=", "660100")]
users = models.execute_kw(db, uid, password, 'res.users', 'search_read', [[]],
                          {'fields': ['id', 'name', 'login']})
# print(users)

for user in users:
    print(user)

# for user in users:
#     user_id = user["id"]
#     print(user_id)
#     result = models.execute_kw(db, uid, password, 'res.users', 'write', [[user_id], {'password': '1'}])
#
#     if result:
#         print(f"Updated password for {user['login']} successfully")
#     else:
#         print("Something went wrong")
