import json
import xmlrpc.client
import pymysql
import time

now = time.strftime('%Y-%m-%d %H:%M:%S')

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


def searchDefault():
    # destination_dbname = 'config_product'
    # destination_user = 'root'
    # destination_password = ''
    # destination_host = 'localhost'
    # destination_port = 3306
    #
    # # เชื่อมต่อกับฐานข้อมูลปลายทาง
    # conn = pymysql.connect(
    #     database=destination_dbname,
    #     user=destination_user,
    #     password=destination_password,
    #     host=destination_host,
    #     port=destination_port
    # )
    # cursor = conn.cursor()
    # sql = "DELETE FROM odoo_config_product_default_value"
    # cursor.execute(sql)
    #
    # alter = "ALTER TABLE odoo_config_product_default_value AUTO_INCREMENT=1;"
    # cursor.execute(alter)

    attr_default_condition = []
    attr_default_template = models.execute_kw(db, uid, password,
                                              'set.attribute.default', 'search_read',
                                              [attr_default_condition], {
                                                  'fields': []})
    for default in attr_default_template:
        print(default)
        default_id = default['id']
        product_id = default['product_category'][0] if isinstance(default['product_category'], list) else None
        attribute_id = default['attribute_id'] if isinstance(default['attribute_id'], list) else None
        attribute_value_id = default['attribute_value_id'][0]
        default_attribute_value_id = json.dumps(default['default_attribute_ids'])
        status = default['active']

    #     insert_sql = """
    #                         INSERT INTO odoo_config_product_default_value (default_id, product_id, attribute_value_id,
    #                             default_attribute_value_id, create_at, update_at)
    #                         VALUES (%s, %s, %s, %s, %s, %s)
    #                     """
    #     cursor.execute(insert_sql, (default_id, product_id, attribute_value_id, default_attribute_value_id, now, now))
    #
    # conn.commit()
    # cursor.close()
    # conn.close()


def searchFormula():
    destination_dbname = 'config_product'
    destination_user = 'root'
    destination_password = ''
    destination_host = 'localhost'
    destination_port = 3306

    # เชื่อมต่อกับฐานข้อมูลปลายทาง
    conn = pymysql.connect(
        database=destination_dbname,
        user=destination_user,
        password=destination_password,
        host=destination_host,
        port=destination_port
    )
    cursor = conn.cursor()
    sql = "DELETE FROM odoo_config_product_formula"
    cursor.execute(sql)

    alter = "ALTER TABLE odoo_config_product_formula AUTO_INCREMENT=1;"
    cursor.execute(alter)
    # pricing.formula
    formula_condition = []
    formula_template = models.execute_kw(db, uid, password,
                                         'pricing.formula', 'search_read',
                                         [formula_condition], {
                                             'fields': []})
    for formula in formula_template:
        print(formula)
        formula_id = formula['id']
        formula_name = formula['name']
        price_based = formula['price_based'] if formula['price_based'] is not None else None
        uom = formula['adv_uom_id'][1] if isinstance(formula['adv_uom_id'], list) else None
        formula_type = formula['compute_price'] if formula['compute_price'] is not None else None
        code = formula['code_py1'] if formula['code_py1'] is not None else None
        try:
            insert_sql = """
                        INSERT INTO odoo_config_product_formula (formula_id, formula_name, price_based, uom,
                            formula_type, code, create_at, update_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
            cursor.execute(insert_sql, (formula_id, formula_name, price_based, uom, formula_type, code, now, now))
        except Exception as e:
            print(f"Exception is {e}")

    conn.commit()
    cursor.close()
    conn.close()


def product_pricelist_item():
    # product.pricelist.item
    destination_dbname = 'config_product'
    destination_user = 'root'
    destination_password = ''
    destination_host = 'localhost'
    destination_port = 3306

    # เชื่อมต่อกับฐานข้อมูลปลายทาง
    conn = pymysql.connect(
        database=destination_dbname,
        user=destination_user,
        password=destination_password,
        host=destination_host,
        port=destination_port
    )
    cursor = conn.cursor()
    sql = "DELETE FROM odoo_config_product_pricelist_item"
    cursor.execute(sql)

    alter = "ALTER TABLE odoo_config_product_pricelist_item AUTO_INCREMENT=1;"
    cursor.execute(alter)
    product_template_condition = [("categ_id", "ilike", "สินค้าสั่งผลิต / ")]
    product_template = models.execute_kw(db, uid, password,
                                         'product.template', 'search_read',
                                         [product_template_condition],
                                         {'fields': ['id']})
    for product in product_template:
        product_id = product['id']
        pricelist_item_condition = [('product_tmpl_id', '=', product_id)]
        pricelist_template = models.execute_kw(db, uid, password,
                                             'product.pricelist.item', 'search_read',
                                             [pricelist_item_condition], {
                                                 'fields': []})
        for pricelist in pricelist_template:
            print(pricelist)
            pricelist_line_id = pricelist['id']
            product_id = pricelist['product_tmpl_id'][0] if isinstance(pricelist['product_tmpl_id'], list) else None
            product_name = pricelist['product_tmpl_id'][1] if isinstance(pricelist['product_tmpl_id'], list) else None
            pricelist_id = pricelist['pricelist_id'][0] if isinstance(pricelist['pricelist_id'], list) else None
            pricelist_name = pricelist['pricelist_id'][1] if isinstance(pricelist['pricelist_id'], list) else None
            compute_price = pricelist['compute_price']
            fixed_price = float(pricelist['fixed_price'])
            price_discount = float(pricelist['price_discount'])
            formula_price = pricelist['percent_price_text']
            try:
                insert_sql = """
                    INSERT INTO odoo_config_product_pricelist_item (pricelist_line_id, product_id, product_name, pricelist_id, pricelist_name, compute_price,
                        fixed_price, price_discount, formula_price, create_at, update_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (pricelist_line_id, product_id, product_name, pricelist_id, pricelist_name, compute_price, fixed_price,
                                            price_discount, formula_price, now, now))
            except Exception as e:
                print(f"Exception is {e}")
            # print(attribute_line_ids)
    conn.commit()
    cursor.close()
    conn.close()


def product_pricelist_type():
    # product.pricelist
    destination_dbname = 'config_product'
    destination_user = 'root'
    destination_password = ''
    destination_host = 'localhost'
    destination_port = 3306

    # เชื่อมต่อกับฐานข้อมูลปลายทาง
    conn = pymysql.connect(
        database=destination_dbname,
        user=destination_user,
        password=destination_password,
        host=destination_host,
        port=destination_port
    )
    cursor = conn.cursor()
    sql = "DELETE FROM odoo_pricelist_type"
    cursor.execute(sql)

    alter = "ALTER TABLE odoo_pricelist_type AUTO_INCREMENT=1;"
    cursor.execute(alter)

    pricelist_type_condition = []
    pricelist_type_template = models.execute_kw(db, uid, password,
                                         'product.pricelist', 'search_read',
                                         [pricelist_type_condition], {
                                             'fields': ['id', 'name', 'discount_policy', 'display_pricelist']})
    for pricelist in pricelist_type_template:
        print(pricelist)
        pricelist_id = pricelist['id']
        pricelist_name = pricelist['name']
        discount_policy = pricelist['discount_policy']
        display_pricelist = pricelist['display_pricelist']

        try:
            insert_sql = """
                INSERT INTO odoo_pricelist_type (pricelist_id, pricelist_name, discount_policy, display_pricelist, create_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (pricelist_id, pricelist_name, discount_policy, display_pricelist, now, now))
        except Exception as e:
            print(f"Exception is {e}")
        # print(attribute_line_ids)
    conn.commit()
    cursor.close()
    conn.close()


def configurable_variant_value():
    # product.template.attribute.value
    destination_dbname = 'config_product'
    destination_user = 'root'
    destination_password = ''
    destination_host = 'localhost'
    destination_port = 3306

    # เชื่อมต่อกับฐานข้อมูลปลายทาง
    conn = pymysql.connect(
        database=destination_dbname,
        user=destination_user,
        password=destination_password,
        host=destination_host,
        port=destination_port
    )
    cursor = conn.cursor()

    # ลบข้อมูลเดิมในตาราง
    sql = "DELETE FROM odoo_config_product_variant_value"
    cursor.execute(sql)

    alter = "ALTER TABLE odoo_config_product_variant_value AUTO_INCREMENT=1;"
    cursor.execute(alter)

    # ค่าพื้นฐานสำหรับ batching
    batch_size = 50000
    offset = 0
    total_records = models.execute_kw(db, uid, password,
                                      'product.template.attribute.value', 'search_count',
                                      [[]])  # นับจำนวนเรคคอร์ดทั้งหมดในโมเดล

    while offset < total_records:
        # ดึงข้อมูลทีละ batch
        variant_value_template = models.execute_kw(db, uid, password,
                                                   'product.template.attribute.value', 'search_read',
                                                   [[]], {
                                                       'fields': ['id', 'product_tmpl_id', 'categ_id', 'attribute_line_id',
                                                                  'attribute_id', 'product_attribute_value_id',
                                                                  'attribute_size_extra', 'price_extra', 'size_based_on',
                                                                  'size_based_on_uom_id', 'cash_price', 'credit_price',
                                                                  'modern_trade', 'customer_price', 'online_price',
                                                                  'discount_cash_price', 'discount_credit_price',
                                                                  'discount_modern_trade', 'discount_customer_price',
                                                                  'discount_online_price'],
                                                       'offset': offset,
                                                       'limit': batch_size
                                                   })
        for variant in variant_value_template:
            # ประมวลผลและเตรียมข้อมูลสำหรับ insert
            varaint_line_id = variant['id']
            product_id = variant['product_tmpl_id'][0] if isinstance(variant['product_tmpl_id'], list) else None
            product_name = variant['product_tmpl_id'][1] if isinstance(variant['product_tmpl_id'], list) else None
            product_cate_id = variant['categ_id'][0] if isinstance(variant['categ_id'], list) else None
            attribute_line_id = variant['attribute_line_id'][0] if isinstance(variant['attribute_line_id'], list) else None
            attribute_id = variant['attribute_id'][0] if isinstance(variant['attribute_line_id'], list) else None
            attribute_value_id = variant['product_attribute_value_id'][0] if isinstance(variant['product_attribute_value_id'], list) else None
            attribute_size_extra = variant['attribute_size_extra'] if variant['attribute_size_extra'] is not None else None
            price_extra = variant['price_extra'] if variant['price_extra'] is not None else None
            formula_id = variant['size_based_on'][0] if isinstance(variant['size_based_on'], list) else None
            formula_name = variant['size_based_on'][1] if isinstance(variant['size_based_on'], list) else None
            uom = variant['size_based_on_uom_id'][1] if isinstance(variant['size_based_on_uom_id'], list) else None
            cash_price = variant['cash_price'] if variant['cash_price'] is not None else None
            credit_price = variant['credit_price'] if variant['credit_price'] is not None else None
            modern_trade_price = variant['modern_trade'] if variant['modern_trade'] is not None else None
            customer_price = variant['customer_price'] if variant['customer_price'] is not None else None
            online_price = variant['online_price'] if variant['online_price'] is not None else None
            discount_cash_price = variant['discount_cash_price'] if variant['discount_cash_price'] is not None else None
            discount_credit_price = variant['discount_credit_price'] if variant['discount_credit_price'] is not None else None
            discount_modern_trade_price = variant['discount_modern_trade'] if variant['discount_modern_trade'] is not None else None
            discount_customer_price = variant['discount_customer_price'] if variant['discount_customer_price'] is not None else None
            discount_online_price = variant['discount_online_price'] if variant['discount_online_price'] is not None else None

            try:
                insert_sql = """
                    INSERT INTO odoo_config_product_variant_value (variant_line_id, product_id, product_name, product_cate_id,
                        attribute_line_id, attribute_id, attribute_value_id, attribute_size_extra, price_extra, formula_id, 
                        formula_name, uom, cash_price, credit_price, modern_trade_price, customer_price, online_price,
                        discount_cash_price, discount_credit_price, discount_modern_trade_price, discount_customer_price, 
                        discount_online_price, create_at, update_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (varaint_line_id, product_id, product_name, product_cate_id, attribute_line_id,
                                            attribute_id, attribute_value_id, attribute_size_extra, price_extra, formula_id,
                                            formula_name, uom, cash_price, credit_price, modern_trade_price, customer_price,
                                            online_price, discount_cash_price, discount_credit_price, discount_modern_trade_price,
                                            discount_customer_price, discount_online_price, now, now))
            except Exception as e:
                print(f"Exception is {e}")

        # เพิ่ม offset
        offset += batch_size

    # Commit การเปลี่ยนแปลงและปิดการเชื่อมต่อ
    conn.commit()
    cursor.close()
    conn.close()


# searchDefault()
searchFormula()
# product_pricelist_item()
# product_pricelist_type()
# configurable_variant_value()

