import xmlrpc.client

# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_DEV'
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
def confirm_immediate_transfer(delivery_name):
    condition = [("name", "=", delivery_name)]

    # ค้นหา stock.picking ด้วยชื่อที่กำหนด
    delivery_template = models.execute_kw(db, uid, password,
                                          'stock.picking', 'search_read',
                                          [condition], {
                                              'fields': ['id', 'name', 'move_ids_without_package', 'state']})

    for deli in delivery_template:
        print(f"Found delivery: ID={deli['id']}, Name={deli['name']}, State={deli['state']}")

        if deli['state'] == 'done':
            print(f"Delivery {deli['name']} is already validated.")
            continue

        button_validate_id = deli['id']
        print('button_validate_id', button_validate_id)

        # ทำการ validate ใบส่งสินค้า (picking)
        try:
            validate_delivery = models.execute_kw(
                db, uid, password, 'stock.picking', 'button_validate', [[button_validate_id]]
            )
            print('validate_deli', validate_delivery)

            # ถ้าผลลัพธ์ของการ validate เป็นการแสดง wizard ให้ทำการ confirm wizard
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

            else:
                print(f"Delivery {deli['name']} is successfully validated without Immediate Transfer.")

        except xmlrpc.client.Fault as e:
            print(f"Error validating delivery {deli['name']}: {str(e)}")


def validate_delivery_test(delivery_name):
    condition = [("name", "=", delivery_name)]

    # ค้นหา stock.picking ด้วยชื่อที่กำหนด
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

        # ทำการ validate ใบส่งสินค้า (picking)
        try:
            action = models.execute_kw(db, uid, password, 'stock.picking', 'button_validate', [[button_validate_id]])

            if action is None or 'context' not in action:
                print("No context received from button_validate")
                continue

            context = action['context'] if action['context'] is not None else {}

            values = {
                'pick_ids': [(6, 0, [button_validate_id])],
                'immediate_transfer_line_ids': [(0, 0, {'to_immediate': True, 'picking_id': button_validate_id})]
            }

            transfer_id = models.execute_kw(db, uid, password, 'stock.immediate.transfer', 'create', [values])
            models.execute_kw(db, uid, password, 'stock.immediate.transfer', 'process', [transfer_id], {'context': context})

        except Exception as e:
            print(f"Exception is : {e}")

# เรียกใช้ฟังก์ชันสำหรับการยืนยัน Immediate Transfer
confirm_immediate_transfer("WH/OUT/00023")
# validate_delivery_test("F4/OUT/00214")
