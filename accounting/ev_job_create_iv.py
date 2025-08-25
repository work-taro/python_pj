import xmlrpc.client
import datetime

# EV
url = 'http://192.168.9.101:8069/'
db = 'ev_uat02'
username = '660100'
password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})


if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


# CREATE DATA
def create_invoice():
    invoice_line_data = [
        (0, 0, {
            'product_id': 11284,
            'price_unit': 100,
            'quantity': 1,
        })
    ]

    data = {
        'partner_id': 137928,
        'journal_id': 1,
        'currency_id': 137,
        'move_type': 'out_invoice',  # ใบแจ้งหนี้ขาย
        'invoice_line_ids': invoice_line_data
    }
    try:
        invoice_id = models.execute_kw(db, uid, password,
                                       'account.move', 'create',
                                       [data])
        print("Invoice created with ID:", invoice_id)

        try:
            models.execute_kw(db, uid, password,
                              'account.move', 'action_post',
                              [[invoice_id]])
            print("Invoice confirmed!")
        except Exception as post_error:
            print("Invoice might have posted, but got response error:", post_error)

    except Exception as create_error:
        print(f"Error during invoice creation: {create_error}")


for i in range(10):
    create_invoice()

# create_invoice()

