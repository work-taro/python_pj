import xmlrpc.client
import datetime

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


# CREATE DATA
def create_data():
    new_data = {
        'partner_id': 137928,
        'warehouse_id': 1,
        'user_id': 63,
        'pricelist_id': 2,
        'payment_check': 7,
        'type_id': 1,

    }
    print(new_data)
    try:

        sale_order_id = models.execute_kw(db, uid, password,
                                          'sale.order', 'create',
                                          [new_data])
        if sale_order_id:
            print("Sale order created with ID:", sale_order_id)
        else:
            print("Failed to create Sale order")

    except Exception as e:
        print(f"Error {e}")


for i in range(10):
    create_data()

# create_data()
