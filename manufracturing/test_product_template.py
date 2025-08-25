import xmlrpc.client

# URL and login credentials
# url = 'http://localhost:8069'
# db = 'tarotest'
# username = 't'
# password = '1'

url = 'http://192.168.9.102:8069'
db = 'KC_DEV'
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


def search_product_variants():
    products = []
    product_option = []
    attribute_values = []
    product_combo = []
    product_variants_ids = []
    product_template_ids = []
    # Corrected condition to be a list of tuples
    condition = [('bom_ids', '!=', False), ('id', '=', 253000)]
    try:
        pd = models.execute_kw(db, uid, password,
                                  'product.product', 'search_read',
                                  [condition], {
                                      'fields': ['id', 'product_tmpl_id', 'product_template_attribute_value_ids', 'display_name' ,'categ_id', 'attribute_line_val_ids', 'combo_option_id']})
        for i in pd:
            print(i)
            print(f"Display Name : {i['display_name']}")
            print(f'Product ID: {i["id"]}')
            product_variants_id = i["id"]
            product_name = i['product_tmpl_id'][1]
            product_template_id = i['product_tmpl_id'][0]
            # print(product_name)
            product_variants_ids.append(product_variants_id)
            product_template_ids.append(product_template_id)
            products.append(product_name)
            product_attr_id = i['product_template_attribute_value_ids']
            product_cate_name = i['categ_id'][1]
            print(f"Product Category : {product_cate_name}")
            attr_line_id = i['attribute_line_val_ids']
            combo_option_id = i['combo_option_id']
            for j in product_attr_id:
                # print(f'Product Attribute ID: {j}')
                product_attr_value_condition = [('id', '=', j)]
                product_attr = models.execute_kw(db, uid, password,
                                          'product.template.attribute.value', 'search_read',
                                          [product_attr_value_condition], {
                                              'fields': []})
                for p in product_attr:
                    # print(p)
                    product_option.append(p['product_attribute_value_id'][1])

            for attr in attr_line_id:
                attr_line_condition = [('id', '=', attr)]
                attr_line = models.execute_kw(db, uid, password,
                                                 'product.attribute.value', 'search_read',
                                                 [attr_line_condition], {
                                                     'fields': []})
                for atbv in attr_line:
                    attribute = atbv['attribute_id'][1]
                    attribute_value = atbv['name']
                    # print(f"Attribute : {attribute} Attribute_value : {attribute_value}")
                    attributes = attribute + ":" + attribute_value
                    # print(attributes)
                    attribute_values.append(str(attributes))

            for combo in combo_option_id:
                combo_option_condition = [('id', '=', combo)]
                combo_option = models.execute_kw(db, uid, password,
                                              'hdc.combo.option', 'search_read',
                                              [combo_option_condition], {
                                                  'fields': []})
                for com in combo_option:
                    # print(com)
                    product_combo.append(com)
        len_of_attribute = len(attribute_values)
        print(f"Length of Attribute : {len_of_attribute}")
        # print(f"Attribute is : {attribute_values}")
        # print(f"Product option is : {product_option}")
        print("Product", products)
        print("Option", product_option)
        print("Product Variants ID", product_variants_ids)
        print("Product Template ID", product_template_ids)
        print("Attribute", attribute_values)
    except Exception as e:
        print(f"Error: {e}")


def search_id_product_id():
    condition = [('name', '=', 'S00384')]
    # condition = [['default_code', "!=", False], ['default_code', "=", "FURN_9666"]]
    users = models.execute_kw(db, uid, password,
                              'sale.order', 'search_read',
                              [condition], {
                                  'fields': []})
    for i in users:
        # print(i)
        so_name = i['name']
        print(f'SO name : {so_name}')
        order_line = i['order_line']
        # print(order_line[0])
        print(f'Order line ID : {order_line}')
        order_line_condition = [('id', '=', order_line[0])]
        order_line = models.execute_kw(db, uid, password,
                                  'sale.order.line', 'search_read',
                                  [order_line_condition], {
                                      'fields': []})
        for j in order_line:
            # print(j)
            # print(j['name'])
            product_id = j['product_id'][0]
            product_name = j['name']
            product_var_condition = [('id', '=', product_id)]
            product_var = models.execute_kw(db, uid, password,
                                           'product.product', 'search_read',
                                           [product_var_condition], {
                                               'fields': []})
            for p in product_var:
                # print(p)
                print(f"Product ID : {p['id']}")


search_product_variants()
# search_id_product_id()
