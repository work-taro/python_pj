import xmlrpc.client
from datetime import datetime, timedelta
import time

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

# List of orders (you can customize this list with your 20 orders)
val_list = [
    {
        'customer_code': "4-01-4293",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8850002022577', 'qty': 12},
            {'product_name': '4902430306058', 'qty': 11},
            {'product_name': '8850002016675', 'qty': 10},
        ],
    },
    {
        'customer_code': "1-07-262",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 5},
            {'product_name': '8850002022577', 'qty': 10},
            {'product_name': '4902430306058', 'qty': 15},
        ],
    },
    {
        'customer_code': "1-12-455",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 17},
            {'product_name': '4902430306058', 'qty': 15},
            {'product_name': '8850002022577', 'qty': 12},
        ],
    },
    {
        'customer_code': "1-08-220",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8850002022577', 'qty': 16},
            {'product_name': '4902430306195', 'qty': 16},
            {'product_name': '8850002016675', 'qty': 17},
        ],
    },
    {
        'customer_code': "1-06-228",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8850002022577', 'qty': 5},
            {'product_name': '4902430306195', 'qty': 15},
            {'product_name': '8850002016675', 'qty': 18},
        ],
    },
    {
        'customer_code': "DB-01150",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 14},
            {'product_name': '4902430450614', 'qty': 15},
            {'product_name': '8850002022577', 'qty': 13},
        ],
    },
    {
        'customer_code': "1-06-321",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 17},
            {'product_name': '4902430450614', 'qty': 18},
            {'product_name': '8850002022577', 'qty': 14},
        ],
    },
    {
        'customer_code': "4-01-3585",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 12},
            {'product_name': '4902430450621', 'qty': 12},
            {'product_name': '8850002022577', 'qty': 12},
        ],
    },
    {
        'customer_code': "4-02-191",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 13},
            {'product_name': '4902430450621', 'qty': 14},
            {'product_name': '8850002022577', 'qty': 10},
        ],
    },
    {
        'customer_code': "1-01-018",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 10},
            {'product_name': '4902430450645', 'qty': 15},
            {'product_name': '8850002022577', 'qty': 16},
        ],
    },
    {
        'customer_code': "1-01-001",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 17},
            {'product_name': '4902430450645', 'qty': 16},
            {'product_name': '8850002022577', 'qty': 10},
        ],
    },
    {
        'customer_code': "1-01-002",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 12},
            {'product_name': '8850002001053', 'qty': 13},
            {'product_name': '8850002022577', 'qty': 12},
        ],
    },
    {
        'customer_code': "1-01-003",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 13},
            {'product_name': '8850002001053', 'qty': 10},
            {'product_name': '8850002022577', 'qty': 11},
        ],
    },
    {
        'customer_code': "1-01-004",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 12},
            {'product_name': '8850002001138', 'qty': 12},
            {'product_name': '8850002022577', 'qty': 10},
        ],
    },
    {
        'customer_code': "1-01-005",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 11},
            {'product_name': '8850002001138', 'qty': 12},
            {'product_name': '8850002022577', 'qty': 12},
        ],
    },
    {
        'customer_code': "1-01-006",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 12},
            {'product_name': '8851020101200', 'qty': 13},
            {'product_name': '8850002022577', 'qty': 13},
        ],
    },
    {
        'customer_code': "1-01-007",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8850002010109', 'qty': 14},
            {'product_name': '8851020101200', 'qty': 16},
            {'product_name': '8850002022577', 'qty': 15},
        ],
    },
    {
        'customer_code': "1-01-008",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 15},
            {'product_name': '8850002010109', 'qty': 15},
            {'product_name': '8850002022577', 'qty': 13},
        ],
    },
    {
        'customer_code': "1-01-009",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 15},
            {'product_name': '8850002010109', 'qty': 13},
            {'product_name': '8850002022577', 'qty': 15},
        ],
    },
    {
        'customer_code': "1-01-010",
        'type_id': "Sale Vat",
        'order_date': "29/05/2024 13:41:54",
        'delivery_date': "29/05/2024",
        'note': "",
        'product': [
            {'product_name': '8851907295882', 'qty': 14},
            {'product_name': '8850002010109', 'qty': 13},
            {'product_name': '8850002022577', 'qty': 12},
        ],
    },

]


def create_multiple_orders(val_list):
    for order_details in val_list:
        search(order_details)
        time.sleep(1)  # Optional: add a delay between creating each order


def search(row):
    table = 'res.partner'
    table2 = 'sale.order.type'
    table3 = 'stock.warehouse'

    # Find partner (customer) ID
    where_partner = [('ref', '=', row['customer_code'])]
    partner_data = models.execute_kw(db, uid, password, table, 'search_read', [where_partner], {'fields': ['id']})
    partner_id = partner_data[0]['id']

    # Find sale order type ID and warehouse ID
    where_type = [('name', '=', row['type_id'])]
    type_data = models.execute_kw(db, uid, password, table2, 'search_read', [where_type],
                                  {'fields': ['id', 'warehouse_id']})
    type_id = type_data[0]['id']
    warehouse_id = type_data[0]['warehouse_id'][0]

    # Find warehouse name and ID
    where_warehouse = [('id', '=', warehouse_id)]
    warehouse_data = models.execute_kw(db, uid, password, table3, 'search_read', [where_warehouse], {'fields': ['id']})
    wh_id = warehouse_data[0]['id']

    create(row, partner_id, type_id, wh_id)


def create(row, partner_id, type_id, wh_id):
    # Convert dates to proper format
    delivery_date = datetime.strptime(row['delivery_date'], "%d/%m/%Y").strftime("%Y-%m-%d")
    order_date = datetime.strptime(row['order_date'], "%d/%m/%Y %H:%M:%S") - timedelta(hours=7)
    order_date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")

    # Prepare data to create sale order
    data = {
        'partner_id': partner_id,
        'user_id': 2129,  # Adjust as needed
        'date_order': order_date_str,
        'pricelist_id': 1,  # Adjust as needed
        'delivery_date': delivery_date,
        'note': row['note'],
        'type_id': type_id,
        'warehouse_id': wh_id,
    }

    # Create sale order
    so_id = models.execute_kw(db, uid, password, 'sale.order', 'create', [data])

    # Create sale order lines
    for product in row['product']:
        product_name = product['product_name']
        qty_product = product['qty']

        # Find product variant ID
        product_condition = [('default_code', '=', product_name)]
        product_variant = models.execute_kw(db, uid, password, 'product.product', 'search_read', [product_condition],
                                            {'fields': ['id']})

        if product_variant:
            product_id = product_variant[0]['id']
            line_data = {
                'order_id': so_id,
                'product_id': product_id,
                'product_uom_qty': qty_product,
            }
            models.execute_kw(db, uid, password, 'sale.order.line', 'create', [line_data])

            # Print confirmation message
            print(f"Created order with ID: {so_id}")

    models.execute_kw(db, uid, password, 'sale.order', 'action_sale_ok', [[so_id]])


# Start creating multiple orders
create_multiple_orders(val_list)
