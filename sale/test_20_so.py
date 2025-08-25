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
        'customer_code': "2-15-016",
        'type_id': "Normal Order",
        'order_date': "26/06/2024 09:10:54",
        'delivery_date': "26/06/2024",
        'note': "",
        'product': [
            {'product_id': '253001	', 'width': 1, 'height': 1, 'qty': 1, },  # for KC_UAT2024
            # {'product_id': '261525', 'width': 1, 'height': 1, 'qty': 3, },    # for KC_DEV
            # {'product_name': '8851020101200', 'qty': 11},
            # {'product_name': '8850002032873', 'qty': 10},
        ],
    },
    # {
    #     'customer_code': "4-03-806",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 5},
    #         {'product_name': '4902430306195', 'qty': 10},
    #         {'product_name': '8850002032873', 'qty': 15},
    #     ],
    # },
    # {
    #     'customer_code': "5-03-001",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 17},
    #         {'product_name': '8851020101200', 'qty': 15},
    #         {'product_name': '8850002032873', 'qty': 12},
    #     ],
    # },
    # {
    #     'customer_code': "1-03-163",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430306195', 'qty': 16},
    #         {'product_name': '8851020101200', 'qty': 16},
    #         {'product_name': '8850002032873', 'qty': 17},
    #     ],
    # },
    # {
    #     'customer_code': "1-07-262",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430276665', 'qty': 5},
    #         {'product_name': '8850002024618', 'qty': 15},
    #         {'product_name': '8850002032873', 'qty': 18},
    #     ],
    # },
    # {
    #     'customer_code': "4-01-4293",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 14},
    #         {'product_name': '4902430306195', 'qty': 15},
    #         {'product_name': '8850002032873', 'qty': 13},
    #     ],
    # },
    # {
    #     'customer_code': "1-12-455",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': 'ZCTTB0032900', 'qty': 17},
    #         {'product_name': '4902430306195', 'qty': 18},
    #         {'product_name': '8850002032873', 'qty': 14},
    #     ],
    # },
    # {
    #     'customer_code': "1-08-220",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430306195', 'qty': 12},
    #         {'product_name': '8851020101200', 'qty': 12},
    #         {'product_name': '8850002032873', 'qty': 12},
    #     ],
    # },
    # {
    #     'customer_code': "4-02-1114",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 13},
    #         {'product_name': '8851020101200', 'qty': 14},
    #         {'product_name': '8850002032873', 'qty': 10},
    #     ],
    # },
    # {
    #     'customer_code': "1-06-228",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430306195', 'qty': 10},
    #         {'product_name': '8851020101200', 'qty': 15},
    #         {'product_name': '8850002032873', 'qty': 16},
    #     ],
    # },
    # {
    #     'customer_code': "DB-01150",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 17},
    #         {'product_name': '8850002032873', 'qty': 16},
    #         {'product_name': '4902430306195', 'qty': 10},
    #     ],
    # },
    # {
    #     'customer_code': "4-01-4912",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 12},
    #         {'product_name': '4902430306195', 'qty': 13},
    #         {'product_name': '8850002032873', 'qty': 12},
    #     ],
    # },
    # {
    #     'customer_code': "4-01-5084",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 13},
    #         {'product_name': '8851020101200', 'qty': 10},
    #         {'product_name': '8850002032873', 'qty': 11},
    #     ],
    # },
    # {
    #     'customer_code': "IM-00237",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 12},
    #         {'product_name': '4902430306195', 'qty': 12},
    #         {'product_name': '8850002032873', 'qty': 10},
    #     ],
    # },
    # {
    #     'customer_code': "4-01-5108",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 11},
    #         {'product_name': '4902430306195', 'qty': 12},
    #         {'product_name': '8850002032873', 'qty': 12},
    #     ],
    # },
    # {
    #     'customer_code': "4-02-870",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430306195', 'qty': 12},
    #         {'product_name': '8851020101200', 'qty': 13},
    #         {'product_name': '8850002032873', 'qty': 13},
    #     ],
    # },
    # {
    #     'customer_code': "DB-01072",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 14},
    #         {'product_name': '8851020101200', 'qty': 16},
    #         {'product_name': '8850002032873', 'qty': 15},
    #     ],
    # },
    # {
    #     'customer_code': "IM-00369",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '8851907295882', 'qty': 15},
    #         {'product_name': '8851020101200', 'qty': 15},
    #         {'product_name': '8850002032873', 'qty': 13},
    #     ],
    # },
    # {
    #     'customer_code': "DA-00048",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430306058', 'qty': 15},
    #         {'product_name': '8851020101200', 'qty': 13},
    #         {'product_name': '8850002032873', 'qty': 15},
    #     ],
    # },
    # {
    #     'customer_code': "IM-00069",
    #     'type_id': "Normal Order",
    #     'order_date': "29/05/2024 13:27:54",
    #     'delivery_date': "29/05/2024",
    #     'note': "",
    #     'product': [
    #         {'product_name': '4902430306058', 'qty': 14},
    #         {'product_name': '8851020101200', 'qty': 13},
    #         {'product_name': '8850002032873', 'qty': 12},
    #     ],
    # },

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
        # print("Product", product)
        product_name = product['product_id']
        qty_product = product['qty']
        width = product['width']
        height = product['height']

        # Find product variant ID
        product_condition = [('id', '=', product_name)]
        product_variant = models.execute_kw(db, uid, password, 'product.product', 'search_read', [product_condition],
                                            {'fields': ['id']})

        if product_variant:
            try:
                product_id = product_variant[0]['id']

                # Use default_get to get default values for the sale.order.line
                default_values = models.execute_kw(db, uid, password, 'sale.order.line', 'default_get',
                                                   [['order_id', 'product_id', 'width', 'height', 'product_uom_qty']])
                default_values.update({
                    'order_id': so_id,
                    'product_id': product_id,
                    # 'x_custom_width_t': width,
                    'width': width,
                    'height': height,
                    'product_uom_qty': qty_product,
                })

                # Debugging output before creation
                print(f"Before creation: {default_values}")

                # Create the sale order line with updated values
                line_id = models.execute_kw(db, uid, password, 'sale.order.line', 'create', [default_values])

                # Verify the line creation
                if line_id:
                    print(f"Created line with ID: {line_id}")

                # Print confirmation message for the order
                print(f"Created order with ID: {so_id}")

            except Exception as e:
                print(f"Error : {e}")


create_multiple_orders(val_list)
