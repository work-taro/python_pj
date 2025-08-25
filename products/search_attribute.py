import xmlrpc.client

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)

attribute_value_array = []

def searchProduct():
    condition = [('id', '=', '252759')]
    products = models.execute_kw(db, uid, password,
                              'product.template', 'search_read',
                              [condition], {
                                  'fields': []})

    for product in products:
        print(product)
        attribute_line_ids = product['attribute_line_ids']
        print(attribute_line_ids)
        for attribute_id in attribute_line_ids:
            # print(attribute_id)
            attribute_condition = [('id', '=', 7096)]
            attributes = models.execute_kw(db, uid, password,
                                         'product.template.attribute.line', 'search_read',
                                         [attribute_condition], {
                                             'fields': []})
            for attribute in attributes:
                print(attribute)
                # attribute_id = attribute['attribute_id'][0]
                # attribute_name = attribute['attribute_id'][1]
                # attribute_value_ids = attribute['value_ids']
                default_value = attribute['default_val']
                if default_value is False:
                    default_value = None
                    print(f"default_value is {default_value}")
                else:
                    print(default_value)
                # # print(attribute_id)
                # # print(attribute)
                # # print("attribute_name", attribute_name)
                # # print("attribute_value_id", attribute_value_ids)
                # for attribute_value_id in attribute_value_ids:
                #     # print(attribute_value_id)
                #     attribute_value_condition = [('id', '=', attribute_value_id)]
                #     attribute_values = models.execute_kw(db, uid, password,
                #                                    'product.attribute.value', 'search_read',
                #                                    [attribute_value_condition], {
                #                                        'fields': []})
                #     for attr_value in attribute_values:
                #         # print(attr_value)
                #
                #         attr_value_name = attr_value['name']
                #         attribute_value_array.append(attr_value_name)
                #         print(attribute_value_array)


searchProduct()
