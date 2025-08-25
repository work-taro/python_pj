import xmlrpc.client

# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# สร้าง ServerProxy สำหรับเชื่อมต่อกับ Odoo
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


# ฟังก์ชันสำหรับค้นหา ID ของ Immediate Transfer และทำการยืนยัน
def testBackOrder(delivery_name):
    condition = [("name", "=", delivery_name)]

    # ค้นหา stock.picking ด้วยชื่อที่กำหนด
    delivery_template = models.execute_kw(db, uid, password,
                                          'stock.picking', 'search_read',
                                          [condition], {
                                              'fields': []})

    for deli in delivery_template:
        # print(deli)
        # print(f"Found delivery: ID={deli['id']}, Name={deli['name']}, State={deli['state']}")

        if deli['state'] == 'done':
            print(f"Delivery {deli['name']} is already validated.")
            continue

        stock_move = deli['move_lines']
        for stock_id in stock_move:
            # print(f"in line delivery {stock_id}")

            condition_stock_move = [("id", "=", stock_id)]
            stock_move_template = models.execute_kw(db, uid, password,
                                                    'stock.move', 'search_read',
                                                    [condition_stock_move], {
                                                        'fields': ['product_id', 'location_id', 'product_uom_qty', 'forecast_availability', 'quantity_done']})
            for s in stock_move_template:
                print(s)
                condition_stock_move_quantity = ["&",["id", "=", stock_id], ["product_id", "=", 222723]]
                stock_move_quantity_template = models.execute_kw(db, uid, password,
                                                                 'stock.move', 'search_read',
                                                                 [condition_stock_move_quantity], {
                                                                     'fields': ['product_id', 'location_id',
                                                                                'product_uom_qty',
                                                                                'forecast_availability',
                                                                                'quantity_done']})
                if stock_move_quantity_template != []:
                    print(f"stock {stock_move_quantity_template}")
                    for up in stock_move_quantity_template:
                        updated_data = {
                            'quantity_done': 30,
                        }
                        updated_quantity = models.execute_kw(db, uid, password,
                                                   'stock.move', 'write',
                                                   [[up['id']], updated_data])
                        print(updated_quantity)
                else:
                    print("None")

        button_validate_id = deli['id']
        # print('button_validate_id', button_validate_id)

        # ทำการ validate ใบส่งสินค้า (picking)
        try:
            validate_delivery = models.execute_kw(
                db, uid, password, 'stock.picking', 'button_validate', [[button_validate_id]]
            )
            print('validate_deli', validate_delivery)
            #
            # # ถ้าผลลัพธ์ของการ validate เป็นการแสดง wizard ให้ทำการ confirm wizard
            if isinstance(validate_delivery, dict) and validate_delivery.get('res_model') == 'stock.immediate.transfer':
                # สร้าง wizard ด้วย context ที่ถูกต้อง
                context = validate_delivery.get('context', {})
                pick_ids = context.get('default_pick_ids', [])

                if pick_ids:
                    print(f"Creating wizard with pick_ids: {pick_ids}")
                    wizard_id = models.execute_kw(
                        db, uid, password, 'stock.immediate.transfer', 'create', [{
                            'pick_ids': pick_ids
                        }]
                    )
                    print(f"Created wizard: ID={wizard_id}")

                    # เช็คสถานะหลังจากการยืนยัน
                    delivery_after_process = models.execute_kw(db, uid, password,
                                                               'stock.picking', 'read',
                                                               [[button_validate_id]], {'fields': ['state']})
                    print(f"Delivery state after process: {delivery_after_process[0]['state']}")

                    if delivery_after_process[0]['state'] == 'done':
                        print(
                            f"Immediate Transfer for delivery {deli['name']} is successfully processed and validated.")
                    else:
                        print(
                            f"Immediate Transfer for delivery {deli['name']} was processed but not validated. Current state: {delivery_after_process[0]['state']}")
                else:
                    print(f"pick_ids not found in context: {context}")

            elif isinstance(validate_delivery, dict) and validate_delivery.get('res_model') == 'stock.backorder.confirmation':
                print("stock.backorder.confirmation")
                # context = validate_delivery.get('context', {})
                # pick_ids = context.get('default_pick_ids', [])
                # if pick_ids:
                #     print(f"Creating wizard with pick_ids: {pick_ids}")
                #     wizard_id = models.execute_kw(
                #         db, uid, password, 'stock.backorder.confirmation', 'create', [{
                #             'pick_ids': pick_ids
                #         }]
                #     )
                #     print(f"Created wizard: ID={wizard_id}")
                #
                #     # Create Backorder
                #     create_backorder = models.execute_kw(
                #         db, uid, password, 'stock.backorder.confirmation', 'process', [wizard_id], {'context': context}
                #     )
                #     # # Cancel Backorder
                #     # cancel_backorder = models.execute_kw(
                #     #     db, uid, password, 'stock.backorder.confirmation', 'process_cancel_backorder', [wizard_id], {'context': context}
                #     # )
                #     print(f"create_backorder {create_backorder}")
                #
                #     # เช็คสถานะหลังจากการยืนยัน
                #     delivery_after_process = models.execute_kw(db, uid, password,
                #                                                'stock.picking', 'read',
                #                                                [[button_validate_id]], {'fields': ['state']})
                #     print(f"Delivery state after process: {delivery_after_process[0]['state']}")
                # else:
                #     print(f"pick_ids not found in context: {context}")
            else:
                print(f"Delivery {deli['name']} is successfully validated.")

        except xmlrpc.client.Fault as e:
            print(f"Error validating delivery {deli['name']}: {str(e)}")

# เรียกใช้ฟังก์ชันสำหรับการยืนยัน Immediate Transfer
testBackOrder("F4/OUT/01017")
