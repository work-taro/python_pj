import xmlrpc.client

# Odoo instance connection details
url = 'http://localhost:8069'
db = 'tarotest'
username = 't'
password = '1'

# url = 'http://192.168.9.102:8069'
# db = 'KC_DEV'
# username = '660100'
# password = 'Kacee2023'

# Authenticate and connect to Odoo via XMLRPC
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if not uid:
    print("Failed to authenticate!")
    exit()


def create_invoice_test():
    sale_order_name = 'SO240500011'  # Replace with your sale order name

    # Find sale order by name
    sale_order = models.execute_kw(db, uid, password,
                                   'sale.order', 'search_read',
                                   [[['name', '=', sale_order_name]]],
                                   {'fields': []})
    if not sale_order:
        print(f"Sale Order '{sale_order_name}' not found.")
        return

    sale_order_id = sale_order[0]['id']
    print(f"Sale Order ID: {sale_order_id}")

    try:
        context = {
            'active_model': 'sale.order',
            'active_ids': [sale_order_id],
            'active_id': sale_order_id,
            'default_journal_id': 'Customer Invoices',
        }
        invoice_vals = {
            'advance_payment_method': 'delivered'
        }

        invoice_id = models.execute_kw(
            db, uid, password,
            'sale.advance.payment.inv',
            'create_invoices',
            [[invoice_vals]],
            {'context': context}
        )
        print('invoice_id', invoice_id)
    except xmlrpc.client.Fault as e:
        print(f'Error: {e}')

    # if invoice_id:
    #     print(f"Invoice Created. ID: {invoice_id}")
    #
    #     # Post the invoice
    #     models.execute_kw(db, uid, password,
    #                       'account.move', 'action_post',
    #                       [[invoice_id]])
    #
    #     print(f"Invoice Posted. ID: {invoice_id}")
    # else:
    #     print("Failed to create invoice.")


create_invoice_test()
