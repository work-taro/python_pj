import xmlrpc.client

# Odoo XML-RPC URL, database, username, password
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# Authenticate with Odoo
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully! UID:", uid)
else:
    print("Connection failed!")


def update_wh(line_id, width, height):
    update_values = {
        'width': width,
        'height': height,
    }

    try:
        # Update the sale order line with the new width and height
        result = models.execute_kw(db, uid, password, 'sale.order.line', 'write',
                                   [[line_id], update_values])
        print("Update result:", result)

        # Read back the updated values to confirm
        updated_records = models.execute_kw(db, uid, password, 'sale.order.line', 'search_read',
                                            [[['id', '=', line_id]], ['width', 'height']])
        print("Updated values:", updated_records)

    except xmlrpc.client.Fault as e:
        print("XML-RPC Fault:", e.faultCode, e.faultString)
    except Exception as e:
        print("Unexpected error:", str(e))


# Example of updating width and height for sale order line with ID 1303
update_wh(1303, 1.000, 1.000)
