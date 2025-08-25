import xmlrpc.client

url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def read_delivery(delivery_name):
    condition = [("name", "=", delivery_name)]

    delivery_template = models.execute_kw(db, uid, password,
                                          'stock.picking', 'search_read',
                                          [condition], {
                                              'fields': []})
    for delivery in delivery_template:
        print(delivery)


def validate_delivery_test(delivery_name):
    condition = [("name", "=", delivery_name)]

    delivery_template = models.execute_kw(db, uid, password,
                                          'stock.picking', 'search_read',
                                          [condition], {
                                              'fields': ['id', 'name', 'move_ids_without_package', 'state']})

    if not delivery_template:
        print(f"No delivery found with the name {delivery_name}")
        return

    for deli in delivery_template:
        print(f"Found delivery: ID={deli['id']}, Name={deli['name']}, State={deli['state']}")

        if deli['state'] == 'done':
            print(f"Delivery {deli['name']} is already validated.")
            continue

        button_validate_id = deli['id']
        print('button_validate_id', button_validate_id)

        try:
            action = models.execute_kw(db, uid, password, 'stock.picking', 'button_validate', [[button_validate_id]])
            print(action['context'])
            # action['context'][0]['default_show_transfers'] = True

            values = {'pick_ids': [(6, 0, [button_validate_id])],
                      'immediate_transfer_line_ids': [(0, 0, {'to_immediate': True, 'picking_id': button_validate_id})]}

            transfer_id = models.execute_kw(db, uid, password, 'stock.immediate.transfer', 'create', [values])
            # print(transfer_id)
            models.execute_kw(db, uid, password, 'stock.immediate.transfer', 'process', [transfer_id],
                              {'context': action['context']})

            delivery_after_process = models.execute_kw(db, uid, password,
                                                       'stock.picking', 'read',
                                                       [[button_validate_id]], {'fields': ['state']})
            print(f"Delivery state after process: {delivery_after_process[0]['state']}")

        except Exception as e:
            print(f"Exception is : {e}")


# validate_delivery_test("F4/OUT/01106")
read_delivery("F4/OUT/01689")
