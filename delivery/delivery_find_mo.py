import xmlrpc.client

url = 'http://192.168.9.102:8069' #https://dev-kc.kaceebest.com/
db = 'dev_kc'
username = '660100'
password = 'Kacee2023'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def find_mo(mo_name):
    condition_mrp = [("name", "=", mo_name)]

    mrp_template = models.execute_kw(db, uid, password,
                                          'mrp.production', 'search_read',
                                          [condition_mrp], {
                                              'fields': []})

    if not mrp_template:
        print(f"No delivery found with the name {mo_name}")
        return

    for deli in mrp_template:
        print(deli)


def findMoWithDelivery(delivery_name):
    condition = [("name", "=", delivery_name)]
    delivery_template = models.execute_kw(db, uid, password,
                                          'stock.picking', 'search_read',
                                          [condition], {
                                              'fields': []})
    for deli in delivery_template:
        print(deli)
        delivery_id = deli["id"]
        # print(f"delivery_id : {delivery_id}")
        stock_move = deli['move_lines']
        # for stock_id in stock_move:
        #     # print(f"in line delivery {stock_id}")
        #
        #     condition_stock_move = [("id", "=", stock_id)]
        #     stock_move_template = models.execute_kw(db, uid, password,
        #                                           'stock.move', 'search_read',
        #                                           [condition_stock_move], {
        #                                               'fields': []})
        #     for s in stock_move_template:
        #         print(s)
        #
        # condition_mo = [('incoming_picking', '=', delivery_id)]
        # mo_list = models.execute_kw(db, uid, password,
        #                             'mrp.production', 'search_read',
        #                             [condition_mo], {'fields': ['id', 'name', 'product_id', 'product_qty', 'state']})
        # if mo_list:
        #     for mo in mo_list:
        #         print(mo)
        # else:
        #     print(f"This delivery {delivery_name} is not have MO")


def findMoWithSO(delivery_name):
    condition = [("name", "=", delivery_name)]
    sale_template = models.execute_kw(db, uid, password,
                                          'sale.order', 'search_read',
                                          [condition], {
                                              'fields': []})
    for sale in sale_template:
        print(sale)
        picking_ids = sale['picking_ids']
        print(f"picking_ids : {picking_ids}")
        order_line_id = sale['order_line']
        for order in order_line_id:
            print(f"order_line_id : {order_line_id}")

            condition_order_line = [("id", "=", order)]
            order_line_template = models.execute_kw(db, uid, password,
                                                  'sale.order.line', 'search_read',
                                                  [condition_order_line], {
                                                      'fields': []})
            for o in order_line_template:
                print(o)

        condition_mo = [('incoming_picking', '=', picking_ids)]
        mo_list = models.execute_kw(db, uid, password,
                                    'mrp.production', 'search_read',
                                    [condition_mo], {'fields': ['id', 'name', 'product_id', 'product_qty', 'state']})
        if mo_list:
            for mo in mo_list:
                print(mo)
        else:
            print(f"This SO {delivery_name} is not have MO")

def findMoWithSO_(delivery_name):
    condition = [("name", "=", delivery_name)]
    sale_template = models.execute_kw(db, uid, password,
                                          'sale.order', 'search_read',
                                          [condition], {
                                              'fields': []})
    for sale in sale_template:
        # print(sale)
        so_number = sale['name']
        picking_ids = sale['picking_ids']
        # print(f"picking_ids : {picking_ids}")
        order_line_id = sale['order_line']
        for order in order_line_id:
            # print(f"order_line_id : {order_line_id}")

            condition_order_line = [("id", "=", order)]
            order_line_template = models.execute_kw(db, uid, password,
                                                  'sale.order.line', 'search_read',
                                                  [condition_order_line], {
                                                      'fields': []})
            for o in order_line_template:
                print(o)

        # condition_mo = [('incoming_picking', '=', picking_ids)]
        condition_mo = [('origin', '=', so_number)]
        mo_list = models.execute_kw(db, uid, password,
                                    'mrp.production', 'search_read',
                                    [condition_mo], {'fields': ['id', 'name', 'product_id', 'product_qty', 'state']})
        if mo_list:
            for mo in mo_list:
                print(mo)
        else:
            print(f"This SO {delivery_name} is not have MO")


def findMoWithDelivery_(delivery_name):
    condition = [("name", "=", delivery_name)]
    delivery_template = models.execute_kw(db, uid, password,
                                          'stock.picking', 'search_read',
                                          [condition], {
                                              'fields': []})  # Can add specific fields here if necessary
    for deli in delivery_template:
        delivery_id = deli["id"]
        stock_move = deli['move_lines']
        for stock_id in stock_move:
            condition_stock_move = [("id", "=", stock_id)]
            stock_move_template = models.execute_kw(db, uid, password,
                                                  'stock.move', 'search_read',
                                                  [condition_stock_move], {
                                                      'fields': ['id', 'is_bom', 'product_type', 'bom_line_id', 'has_tracking']})  # Can add specific fields here if necessary
            for s in stock_move_template:
                valid_fields = {}
                for field, value in s.items():
                    try:
                        # Accessing each field, handle potential errors
                        valid_fields[field] = value
                    except Exception as e:
                        print(f"Error with field '{field}': {e}")
                print(valid_fields)  # Only shows valid fields


# find_mo("WH/MO/01184")
# findMoWithDelivery("WH/OUT/00027")
# findMoWithDelivery_("F4/OUT/00967")
# findMoWithSO("S01610")
# findMoWithSO_("S01417")
