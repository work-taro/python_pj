import xmlrpc.client

# Odoo instance connection details
# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_DEV'
username = '660100'
password = 'Kacee2023'

# Authenticate and connect to Odoo via XMLRPC
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if not uid:
    print("Failed to authenticate!")
    exit()


def test():
    sale_order_name = 'S00365'  # Replace with your sale order name

    # Find sale order by name
    sale_order = models.execute_kw(db, uid, password,
                                   'sale.order', 'search_read',
                                   [[['name', '=', sale_order_name]]],
                                   {'fields': []})
    if not sale_order:
        print(f"Sale Order '{sale_order_name}' not found.")
        return

    sale_order_id = sale_order[0]['id']
    sale_order_invoice_status = sale_order[0]['invoice_status']
    print(f"Sale Order ID: {sale_order_id}")
    print(f'invoice status: {sale_order_invoice_status}')
    if sale_order_id:
        if sale_order_invoice_status == 'to invoice':
            try:
                payment_id = models.execute_kw(db, uid, password, 'sale.advance.payment.inv', 'create', [{
                    'advance_payment_method': 'delivered',
                    # 'create_uid': uid,
                    # 'currency_id': 137,
                    # 'deduct_down_payments': True,
                    # 'deposit_account_id': False,
                    # 'fixed_amount': 0.0,
                    # 'has_down_payments': False,
                    # 'write_uid': uid,
                }])

                if payment_id:
                    context = {'active_ids': [sale_order_id]}
                    create_invoices = models.execute_kw(db, uid, password, 'sale.advance.payment.inv', 'create_invoices', [payment_id], {'context': context})
                    print('Result:', create_invoices)
                    print('type', create_invoices.get('type'))
                    # Handle further actions or logging if needed
                    if create_invoices and isinstance(create_invoices, dict) and create_invoices.get('type') == 'ir.actions.act_window_close':
                        sale_order = models.execute_kw(db, uid, password, 'sale.order', 'read', [[sale_order_id], ['invoice_ids']])
                        if sale_order:
                            invoice_id = sale_order[0]['invoice_ids'][0]
                            try:
                                action_post = models.execute_kw(db, uid, password, 'account.move', 'action_post', [invoice_id])
                                print("Invoice confirmed and status changed to Posted.")
                            except xmlrpc.client.Fault as e:
                                print(f"action_post Error: {e}")
                            invoice = models.execute_kw(db, uid, password, 'account.move', 'read', [[invoice_id], ['name', 'highest_name']])
                            if invoice:
                                invoice_name = invoice[0]['name']
                                print('Invoice name is : ', invoice_name)
                                # เรียกใช้เมธอด action_post เพื่อ Confirm ใบแจ้งหนี้
            except Exception as e:
                print("Error:", e)
        elif sale_order_invoice_status == 'invoiced':
            print('This Sale Order has been already create invoice')
        elif sale_order_invoice_status == 'no':
            print('This Sale Order status is Nothing to invoice')
    else:
        print('Do not have Sale Order ID : ', sale_order_id)

    exit()


test()
