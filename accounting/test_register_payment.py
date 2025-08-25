import xmlrpc.client
import datetime as dt

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


def register_payment():
    invoice_number = 'OL/2024/05/0006'

    # Find sale order by name
    invoice = models.execute_kw(db, uid, password, 'account.move', 'search_read', [[['name', '=', invoice_number]]], {'fields': []})
    if invoice:
        invoice_id = invoice[0]['id']
        invoice_status = invoice[0]['state']
        invoice_payment_status = invoice[0]['payment_state']
        print(invoice_id)

        if invoice_status == 'posted':
            print('Invoice status : posted')
            if invoice_payment_status == 'not_paid':
                print('Payment Status : Not Paid')
                payment_data = {
                    'journal_id': 262,  # แทนด้วย ID ของ Journal
                    # 'payment_method_id': 16,  # แทนด้วย ID ของวิธีการชำระเงิน
                    # 'amount': 100.0,  # จำนวนเงินที่ชำระ
                    # 'currency_id': 137,  # สกุลเงินที่ชำระ
                    'payment_date': str(dt.date.today()),  # วันที่ที่ชำระเงิน
                    # 'communication': "",  # Memo
                }
                # เรียกใช้เมธอด action_register_payment เพื่อลงทะเบียนการชำระเงิน
                payment_register_id = models.execute_kw(db, uid, password, 'account.payment.register', 'create', [payment_data], {'context': {'active_model': 'account.move', 'active_ids': [invoice_id]}})
                action_create_payments = models.execute_kw(db, uid, password, 'account.payment.register', 'action_create_payments', [payment_register_id])
                if action_create_payments:
                    print("Payment registered successfully.")
                else:
                    print("Failed to register payment.")
            elif invoice_payment_status == 'in_payment':
                print('Payment Status : In Payment')
            elif invoice_payment_status == 'paid':
                print('Payment Status : Paid')
            elif invoice_payment_status == 'partial':
                print('Payment Status : Partially Paid')
            elif invoice_payment_status == 'reversed':
                print('Payment Status : Reversed')
            elif invoice_payment_status == 'invoicing_legacy':
                print('Payment Status : Invoicing App Legacy')

        elif invoice_status == 'draft':
            print('Invoice status : Draft.')
        elif invoice_status == 'cancel':
            print('Invoice status : Cancel.')


register_payment()
