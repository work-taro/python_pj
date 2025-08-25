import xmlrpc.client

url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# Connect to the server
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def validate_mo_test(mo_number):
    condition = [("name", "=", mo_number)]

    mrp_template = models.execute_kw(db, uid, password,
                                     'mrp.production', 'search_read',
                                     [condition], {
                                         'fields': []})

    if not mrp_template:
        print(f"No MO found with the name {mo_number}")
        return

    for m in mrp_template:
        print("m", m)
        print(f"Found MO: ID={m['id']}, Name={m['name']}")

        if m['state'] == 'done':
            print(f"MO {m['name']} is already validated.")
            continue

        button_validate_id = m['id']
        print('button_validate_id', button_validate_id)

        try:
            # Validate the MO
            action = models.execute_kw(db, uid, password, 'mrp.production', 'button_mark_done', [[button_validate_id]])
            print('action_context', action['context'])

            values = {'mo_ids': [(6, 0, [button_validate_id])],
                      'immediate_production_line_ids': [
                          (0, 0, {'to_immediate': True, 'production_id': button_validate_id})],
                      'show_productions': False,
                      'create_uid': uid,
                      }

            mark_done_id = models.execute_kw(db, uid, password, 'mrp.immediate.production', 'create', [values])
            print('mark_done_id', mark_done_id)
            models.execute_kw(db, uid, password, 'mrp.immediate.production', 'process', [mark_done_id], {'context': action['context']})

            mo_after_process = models.execute_kw(db, uid, password,
                                                 'mrp.production', 'read', [[button_validate_id]],
                                                 {'fields': ['state']})
            print(f"MO state after process: {mo_after_process[0]['state']}")

        except Exception as e:
            print(f"Exception is : {e}")


validate_mo_test("WH/MO/01534")
