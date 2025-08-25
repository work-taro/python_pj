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


def createInvoice():
    order_id = 47 # Replace with the sale.order ID

    # สร้างใบแจ้งหนี้จากใบสั่งขาย
    # สร้างการชำระเงิน
    payment_id = models.execute_kw(db, uid, password, 'sale.advance.payment.inv', 'create', [{
        'advance_payment_method': 'delivered',
        'create_uid': uid,
        'currency_id': 137,
        'deduct_down_payments': True,
        'deposit_account_id': False,
        'fixed_amount': 0.0,
        'has_down_payments': False,
        'write_uid': uid,
    }])
    if payment_id:
        print("Payment created successfully.")
        # สร้างใบแจ้งหนี้
        context = {'active_ids': [order_id]}
        create_invoices = models.execute_kw(db, uid, password, 'sale.advance.payment.inv', 'create_invoices', [payment_id], {'context': context})
        if create_invoices and isinstance(create_invoices, dict) and create_invoices.get('type') == 'ir.actions.act_window_close':
            sale_order = models.execute_kw(db, uid, password, 'sale.order', 'read', [[order_id], ['invoice_ids']])
            if sale_order:
                invoice_id = sale_order[0]['invoice_ids'][0]
                invoice = models.execute_kw(db, uid, password, 'account.move', 'read', [[invoice_id], ['name', 'highest_name']])
                if invoice:
                    invoice_name = invoice[0]['highest_name']
                    # เรียกใช้เมธอด action_post เพื่อ Confirm ใบแจ้งหนี้
                    try:
                        action_post = models.execute_kw(db, uid, password, 'account.move', 'action_post', [invoice_id])
                        print("Invoice confirmed and status changed to Posted.")
                    except xmlrpc.client.Fault as e:
                        print(f"action_post Error: {e}")
                    # ระบุข้อมูลการชำระเงิน

                    # payment_data = {
                    #     'journal_id': 262,  # แทนด้วย ID ของ Journal
                    #     'payment_method_id': 16,  # แทนด้วย ID ของวิธีการชำระเงิน
                    #     # 'amount': 100.0,  # จำนวนเงินที่ชำระ
                    #     # 'currency_id': 137,  # สกุลเงินที่ชำระ
                    #     'payment_date': str(dt.date.today()),  # วันที่ที่ชำระเงิน
                    #     # 'communication': "",  # Memo
                    # }
                    # # เรียกใช้เมธอด action_register_payment เพื่อลงทะเบียนการชำระเงิน
                    # payment_register_id = models.execute_kw(db, uid, password, 'account.payment.register', 'create', [payment_data], {'context': {'active_model': 'account.move', 'active_ids': [invoice_id]}})
                    # action_create_payments = models.execute_kw(db, uid, password, 'account.payment.register', 'action_create_payments', [payment_register_id])
                    # if action_create_payments:
                    #     print("Payment registered successfully.")
                    # else:
                    #     print("Failed to register payment.")
            else:
                print("Failed to confirm invoice.")
        else:
            print("Invoice is not this type : ir.actions.act_window_close")
    else:
        print("Failed to created payment.")

    exit()


createInvoice()
