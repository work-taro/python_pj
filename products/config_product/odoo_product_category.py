import xmlrpc.client
import pandas as pd
import pymysql.cursors
import re
import json
import os
import sys
import time
from threading import Thread

now = time.strftime('%Y-%m-%d %H:%M:%S')


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS2
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


def read_config(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            if line.strip():
                key, value = line.strip().split('=')
                config[key] = value
    return config


config = read_config(resource_path('odoo_config.txt'))

url = config.get('url')
db = config.get('db')
username = config.get('username')
password = config.get('password')
# moduleKC = config.get('module')

# # KC
# url = 'http://192.168.9.102:8069'
# db = 'KC_UAT_27022024'
# username = '660100'
# password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
conn = None

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def dbCenter():
    # Center
    # conn = pymysql.connect(host='127.0.0.1', port=3306, cursorclass=pymysql.cursors.DictCursor, database='kacee_center', user='kaceecent_er', password='0gC186!S}Bj6rr')

    # Dev
    # conn = pymysql.connect(host='192.168.2.12', port=3306, cursorclass=pymysql.cursors.DictCursor,
    #                        database='kacee_center_dev', user='kacee', password='K_cee2022')

    # MyPC
    conn = pymysql.connect(host='localhost', port=3306, cursorclass=pymysql.cursors.DictCursor,
                           database='config_product', user='root', password='')
    return conn


def product_template_conn_db():
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = "DELETE FROM odoo_config_products"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_products AUTO_INCREMENT=1;"
            cursor.execute(alter)

            sql1 = "DELETE FROM odoo_config_product_attribute_line"
            cursor.execute(sql1)

            alter1 = "ALTER TABLE odoo_config_product_attribute_line AUTO_INCREMENT=1;"
            cursor.execute(alter1)

            # product_template_condition = ["|", ["categ_id", "=", "สินค้าสั่งผลิต / ม่านม้วน"], ["categ_id", "=", "สินค้าสั่งผลิต / เมจิก"]]
            product_template_condition = [("categ_id", "ilike", "สินค้าสั่งผลิต / ")]
            product_template = models.execute_kw(db, uid, password,
                                                 'product.template', 'search_read',
                                                 [product_template_condition],
                                                 {'fields': ['id', 'name', 'categ_id', 'uom_id', 'width_min',
                                                             'width_max',
                                                             'height_min', 'height_max', 'route_ids',
                                                             'create_date', '__last_update', 'size_based_on',
                                                             'attribute_line_ids']})
            for product in product_template:
                # print(product)
                product_id = product['id']
                product_name = product['name']
                product_category_id = product['categ_id'][0] if isinstance(product['categ_id'], list) else None
                product_category_name = product['categ_id'][1] if isinstance(product['categ_id'], list) else None
                uom = product.get('uom_id')[1] if isinstance(product['uom_id'], list) else None
                width_min = product.get('width_min')
                width_max = product.get('width_max')
                height_min = product.get('height_min')
                height_max = product.get('height_max')
                route_ids = json.dumps(product.get('route_ids'))
                create_date = product.get('create_date')
                last_update = product.get('__last_update')
                formula_id = product['size_based_on'][0] if isinstance(product['size_based_on'], list) else None
                formula_name = product['size_based_on'][1] if isinstance(product['size_based_on'], list) else None
                attribute_line_ids = json.dumps(product['attribute_line_ids'])
                loop_line = product['attribute_line_ids']
                # print(json.dumps(attribute_line_ids))

                try:
                    insert_sql = """
                        INSERT INTO odoo_config_products (product_id, name, product_cate_id, uom, width_min, width_max, height_min, height_max, route_ids,
                        formula_id,  formula_name, create_date, last_update, attribute_line, create_at, update_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        product_id, product_name, product_category_id, uom,
                        width_min, width_max, height_min, height_max, route_ids, formula_id, formula_name,
                        create_date, last_update, attribute_line_ids, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
                # print(attribute_line_ids)
                count_dict = {}
                for a in loop_line:
                    if a in count_dict:
                        count_dict[a] += 1
                    else:
                        count_dict[a] = 1

                # ค้นหาตัวเลขที่ซ้ำ
                duplicates = {key: value for key, value in count_dict.items() if value > 1}

                if duplicates:
                    print("พบเลขซ้ำ:")
                    for num, count in duplicates.items():
                        print(f"เลข {num} ซ้ำ {count} ครั้ง")
                else:
                    # print("ไม่มีเลขซ้ำ")
                    for id_attribute_line in loop_line:
                        # print(id_attribute_line)
                        attr_line_condition = [("id", "=", id_attribute_line)]
                        attr_line = models.execute_kw(db, uid, password,
                                                      'product.template.attribute.line', 'search_read',
                                                      [attr_line_condition],
                                                      {'fields': []})

                        for attr in attr_line:
                            # print(attr)
                            attr_line_id = attr['id']
                            product_template_id = attr['product_tmpl_id'][0] if isinstance(attr['product_tmpl_id'], list) else None
                            attr_id = attr['attribute_id'][0] if isinstance(attr['attribute_id'], list) else None
                            attr_value_id = json.dumps(attr['value_ids'])
                            default_value_id = attr['default_val'][0] if isinstance(attr['default_val'], list) else None
                            custom = attr['custom']
                            # if default_value_id is False:
                            #     default_value_id = None
                            # else:
                            #     default_value_id = attr['default_val'][0]
                            try:
                                insert_sql = """
                                    INSERT INTO odoo_config_product_attribute_line (attribute_line_id, product_id, attribute_id, attribute_value_ids, default_value_id, custom, create_at, update_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                cursor.execute(insert_sql,
                                               (attr_line_id, product_template_id, attr_id, attr_value_id, default_value_id, custom, now, now))
                            except Exception as e:
                                print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def location_conn_db():
    try:
        conn = dbCenter()
        if conn.connect:
            # print("connect")
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = "DELETE FROM odoo_config_product_location"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_location AUTO_INCREMENT=1;"
            cursor.execute(alter)

            location_condition = []
            locaiton_template = models.execute_kw(db, uid, password,
                                                  'stock.location.route', 'search_read',
                                                  [location_condition],
                                                  {'fields': []})
            for location in locaiton_template:
                # print(location)
                location_id = location['id']
                location_name = location['name']

                try:
                    insert_sql = """
                            INSERT INTO odoo_config_product_location (location_id, name, create_at, update_at)
                            VALUES (%s, %s, %s, %s)
                        """
                    cursor.execute(insert_sql, (location_id, location_name, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def attribute_conn_db():
    try:
        conn = dbCenter()
        if conn.connect:
            # print("connect")
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = "DELETE FROM odoo_config_product_attribute"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_attribute AUTO_INCREMENT=1;"
            cursor.execute(alter)

            product_attribute_condition = []
            product_attribute_template = models.execute_kw(db, uid, password,
                                                           'product.attribute', 'search_read',
                                                           [product_attribute_condition], {
                                                               'fields': ['id', 'name', 'value_ids',
                                                                          'attribute_line_ids']})
            for product_attribute in product_attribute_template:
                # print(product_attribute)
                attribute_id = product_attribute['id']
                name = product_attribute['name']
                value_ids = json.dumps(product_attribute['value_ids'])
                attribute_line = json.dumps(product_attribute['attribute_line_ids'])
                try:
                    insert_sql = """
                                            INSERT INTO odoo_config_product_attribute (attribute_id, name, value_ids, attribute_line, create_at, update_at)
                                            VALUES (%s, %s, %s, %s, %s, %s)
                                        """
                    cursor.execute(insert_sql, (attribute_id, name, value_ids, attribute_line, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def attribute_value_conn_db():
    try:
        conn = dbCenter()
        if conn.connect:
            # print("connect")
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = "DELETE FROM odoo_config_product_attribute_value"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_attribute_value AUTO_INCREMENT=1;"
            cursor.execute(alter)

            product_attribute_value_condition = []
            product_attribute_value_template = models.execute_kw(db, uid, password,
                                                                 'product.attribute.value', 'search_read',
                                                                 [product_attribute_value_condition],
                                                                 {'fields': ['id', 'name', 'attribute_id', 'related_product_id']})
            for product_attribute_value in product_attribute_value_template:
                # print(product_attribute_value)
                attribute_value_id = product_attribute_value['id']
                name = str(product_attribute_value['name'])
                attribute_id = product_attribute_value['attribute_id'][0] if isinstance(product_attribute_value['attribute_id'], list) else None
                multi_combo_id = product_attribute_value.get('related_product_id', None)  # ถ้าไม่มี key ให้เป็น None
                if isinstance(multi_combo_id, list):  # ถ้าเป็น list
                    if multi_combo_id:  # ถ้า list ไม่ว่าง
                        multi_combo_id = multi_combo_id[0]  # ใช้ค่าแรกใน array
                    else:
                        multi_combo_id = None

                condition_multi_combo = [('id', '=', multi_combo_id)]
                attr_value_tem_s = models.execute_kw(db, uid, password, 'multi.combo.option', 'search_read',
                                                     [condition_multi_combo], {
                                                         'fields': ['product_id']})
                related_product_id = None
                for multi_combo in attr_value_tem_s:
                    # print(multi_combo)
                    product_variants_id = multi_combo['product_id'][0] if isinstance(multi_combo['product_id'], list) else None

                    condition_product_variant = [('id', '=', product_variants_id)]
                    pro_variant = models.execute_kw(db, uid, password, 'product.product', 'search_read',
                                                [condition_product_variant],
                                                {
                                                    'fields': ['product_tmpl_id']})
                    for p in pro_variant:
                        if isinstance(p['product_tmpl_id'], list):
                            related_product_id = p['product_tmpl_id'][0]
                try:
                    insert_sql = """
                                                INSERT INTO odoo_config_product_attribute_value (attribute_value_id, name, attribute_id, related_product_id, create_at, update_at)
                                                VALUES (%s, %s, %s, %s, %s, %s)
                                            """
                    cursor.execute(insert_sql, (attribute_value_id, name, attribute_id, related_product_id, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def default_value_conn_db():
    try:
        conn = dbCenter()
        if conn.connect:
            # print("connect")
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = "DELETE FROM odoo_config_product_default_value"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_default_value AUTO_INCREMENT=1;"
            cursor.execute(alter)

            attr_default_condition = []
            attr_default_template = models.execute_kw(db, uid, password,
                                                      'set.attribute.default', 'search_read',
                                                      [attr_default_condition], {
                                                          'fields': []})
            for default in attr_default_template:
                # print(default)
                default_id = default['id']
                product_id = default['product_category'][0] if isinstance(default['product_category'], list) else None
                attribute_id = default['attribute_id'] if isinstance(default['attribute_id'], list) else None
                attribute_value_id = default['attribute_value_id'][0] if isinstance(default['attribute_value_id'], list) else None
                default_attribute_value_id = json.dumps(default['default_attribute_ids'])
                status = default['active']

                insert_sql = """
                                    INSERT INTO odoo_config_product_default_value (default_id, product_id, attribute_value_id, 
                                        default_attribute_value_id, create_at, update_at)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """
                cursor.execute(insert_sql, (default_id, product_id, attribute_value_id, default_attribute_value_id, now, now))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def searchFormula():
    try:
        conn = dbCenter()
        if conn.connect:
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
                # print(formula)
                formula_id = formula['id']
                formula_name = formula['name']
                price_based = formula['price_based'] if formula['price_based'] is not None else None
                uom = formula['uom_id'][1] if isinstance(formula['uom_id'], list) else None
                formula_type = formula['compute_price'] if formula['compute_price'] is not None else None
                dimension = formula['dimension']
                code = formula['code_py1'] if formula['code_py1'] is not False else None
                try:
                    insert_sql = """
                                INSERT INTO odoo_config_product_formula (formula_id, formula_name, price_based, uom,
                                    formula_type, dimension, code, create_at, update_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                    cursor.execute(insert_sql, (formula_id, formula_name, price_based, uom, formula_type, dimension, code, now, now))
                except Exception as e:
                    print(f"Exception is {e}")

            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def product_pricelist_item():
    # product.pricelist.item
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            sql = "DELETE FROM odoo_config_product_pricelist_item"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_pricelist_item AUTO_INCREMENT=1;"
            cursor.execute(alter)
            pricelist_item_condition = ["|", ["product_tmpl_id.categ_id", "ilike", "สินค้าสั่งผลิต / "], ["product_tmpl_id.categ_id", "ilike", "วัตถุดิบ / "]]
            pricelist_template = models.execute_kw(db, uid, password,
                                                 'product.pricelist.item', 'search_read',
                                                 [pricelist_item_condition], {
                                                     'fields': []})
            for pricelist in pricelist_template:
                # print(pricelist)
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
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def product_pricelist_type():
    # product.pricelist
    try:
        conn = dbCenter()
        if conn.connect:
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
                # print(pricelist)
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
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def configurable_variant_value():
    # product.template.attribute.value
    try:
        conn = dbCenter()
        if conn.connect:
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
                                              [[]])

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
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()


def group_chapklang():
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            # ลบข้อมูลเดิมในตาราง
            sql = "DELETE FROM odoo_config_product_categ_caught_group"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_categ_caught_group AUTO_INCREMENT=1;"
            cursor.execute(alter)

            chapklang_condition = []
            chapklang_template = models.execute_kw(db, uid, password,
                                                   'categ.caught', 'search_read',
                                                   [chapklang_condition], {
                                                       'fields': []})
            for chapklang in chapklang_template:
                categ_caught_id = chapklang['id']
                categ_caught_name = chapklang['name']
                display_name = chapklang['display_name']
                try:
                    insert_sql = """
                        INSERT INTO odoo_config_product_categ_caught_group (categ_caught_id, categ_caught_name, display_name, 
                        create_at, update_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (categ_caught_id, categ_caught_name, display_name, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.open:
            conn.close()


def rate_chapklang():
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            # ลบข้อมูลเดิมในตาราง
            sql = "DELETE FROM odoo_config_product_rate_chapklang"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_rate_chapklang AUTO_INCREMENT=1;"
            cursor.execute(alter)

            rate_chapklang_condition = []
            rate_chapklang_template = models.execute_kw(db, uid, password,
                                                        'caught.formula', 'search_read',
                                                        [rate_chapklang_condition], {
                                                            'fields': []})
            for rateChapklang in rate_chapklang_template:
                rateChapklang_id = rateChapklang['id']
                rateChapklang_name = rateChapklang['name']
                categ_caught_id = rateChapklang['categ_caught_id'][0] if isinstance(rateChapklang['categ_caught_id'],
                                                                                    list) else None
                categ_caught_name = rateChapklang['categ_caught_id'][1] if isinstance(rateChapklang['categ_caught_id'],
                                                                                      list) else None
                min = rateChapklang['min']
                max = rateChapklang['max']
                qty = rateChapklang['qty']
                active = rateChapklang['active']
                try:
                    insert_sql = """
                        INSERT INTO odoo_config_product_rate_chapklang (id, name, categ_caught_id, categ_caught_name, 
                        min, max, qty, active, create_at, update_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql,
                                   (rateChapklang_id, rateChapklang_name, categ_caught_id, categ_caught_name,
                                    min, max, qty, active, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()

    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.open:
            conn.close()


def price_chapklang():
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            # ลบข้อมูลเดิมในตาราง
            sql = "DELETE FROM odoo_config_product_price_chapklang"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_price_chapklang AUTO_INCREMENT=1;"
            cursor.execute(alter)

            price_chapklang_condition = []
            price_chapklang_template = models.execute_kw(db, uid, password,
                                                         'caught.rate', 'search_read',
                                                         [price_chapklang_condition], {
                                                             'fields': []})
            for priceChapklang in price_chapklang_template:
                price_chapklang_id = priceChapklang['id']
                product_id = priceChapklang['product_tmpl_id'][0] if isinstance(priceChapklang['product_tmpl_id'],
                                                                                list) else None
                product_name = priceChapklang['product_tmpl_id'][1] if isinstance(priceChapklang['product_tmpl_id'],
                                                                                  list) else None
                name = priceChapklang['name']
                price = priceChapklang['price']
                set_default = priceChapklang['set_default']
                formula_id = priceChapklang['formula'][0] if isinstance(priceChapklang['formula'], list) else None

                try:
                    insert_sql = """
                            INSERT INTO odoo_config_product_price_chapklang (id, product_id, product_name, name,
                            price, set_default, formula_id, create_at, update_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                    cursor.execute(insert_sql,
                                   (price_chapklang_id, product_id, product_name, name, price, set_default, formula_id,
                                    now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()

    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.open:
            conn.close()


def price_switch():
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            # ลบข้อมูลเดิมในตาราง
            sql = "DELETE FROM odoo_config_product_price_switch"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_price_switch AUTO_INCREMENT=1;"
            cursor.execute(alter)

            price_switch_condition = []
            price_switch_template = models.execute_kw(db, uid, password,
                                                      'switch.rate', 'search_read',
                                                      [price_switch_condition], {
                                                          'fields': []})
            for priceSwitch in price_switch_template:
                price_switch_id = priceSwitch['id']
                product_id = priceSwitch['product_tmpl_id'][0] if isinstance(priceSwitch['product_tmpl_id'],
                                                                             list) else None
                product_name = priceSwitch['product_tmpl_id'][1] if isinstance(priceSwitch['product_tmpl_id'],
                                                                               list) else None
                name = priceSwitch['name']
                price = priceSwitch['price']
                set_default = priceSwitch['set_default']
                formula_id = priceSwitch['formula'][0] if isinstance(priceSwitch['formula'], list) else None

                try:
                    insert_sql = """
                            INSERT INTO odoo_config_product_price_switch (id, product_id, product_name, name,
                            price, set_default, formula_id, create_at, update_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                    cursor.execute(insert_sql,
                                   (price_switch_id, product_id, product_name, name, price, set_default, formula_id,
                                    now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()

    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.open:
            conn.close()


def price_remote():
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            # ลบข้อมูลเดิมในตาราง
            sql = "DELETE FROM odoo_config_product_price_remote"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_config_product_price_remote AUTO_INCREMENT=1;"
            cursor.execute(alter)

            price_remote_condition = []
            price_remote_template = models.execute_kw(db, uid, password,
                                                      'remote.rate', 'search_read',
                                                      [price_remote_condition], {
                                                          'fields': []})
            for priceRemote in price_remote_template:
                price_remote_id = priceRemote['id']
                product_id = priceRemote['product_tmpl_id'][0] if isinstance(priceRemote['product_tmpl_id'],
                                                                             list) else None
                product_name = priceRemote['product_tmpl_id'][1] if isinstance(priceRemote['product_tmpl_id'],
                                                                               list) else None
                name = priceRemote['name']
                price = priceRemote['price']
                set_default = priceRemote['set_default']
                formula_id = priceRemote['formula'][0] if isinstance(priceRemote['formula'], list) else None

                try:
                    insert_sql = """
                            INSERT INTO odoo_config_product_price_remote (id, product_id, product_name, name,
                            price, set_default, formula_id, create_at, update_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                    cursor.execute(insert_sql,
                                   (price_remote_id, product_id, product_name, name, price, set_default, formula_id,
                                    now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()

    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.open:
            conn.close()


def product_category():
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor()
            # ลบข้อมูลเดิมในตาราง
            sql = "DELETE FROM odoo_product_category"
            cursor.execute(sql)

            alter = "ALTER TABLE odoo_product_category AUTO_INCREMENT=1;"
            cursor.execute(alter)

            productCategory_condition = []
            productCategory_template = models.execute_kw(db, uid, password,
                                                         'product.category', 'search_read',
                                                         [productCategory_condition], {
                                                             'fields': []})
            for productCate in productCategory_template:
                productCate_displayName = productCate['complete_name']
                productCate_name = productCate['name']
                parent_category = productCate['parent_id'][1] if isinstance(productCate['parent_id'], list) else None
                odoo_id = productCate['id']
                try:
                    insert_sql = """
                            INSERT INTO odoo_product_category (display_name, name, parent_category, odoo_id, 
                                create_at, update_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                    cursor.execute(insert_sql,
                                   (productCate_displayName, productCate_name, parent_category, odoo_id, now, now))
                except Exception as e:
                    print(f"Exception is {e}")
            conn.commit()
            cursor.close()
            conn.close()

    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.open:
            conn.close()


def process():
    # product, attribute, attribute_value, location
    try:
        print("Start product_template_conn_db")
        product_template_conn_db()
        print("Finish product_template_conn_db")
    except Exception as e:
        print(f"Error in product_template_conn_db: {e}")
    try:
        print("Start location_conn_db")
        location_conn_db()
        print("Finish location_conn_db")
    except Exception as e:
        print(f"Error in location_conn_db: {e}")
    try:
        print("Start attribute_conn_db")
        attribute_conn_db()
        print("Finish attribute_conn_db")
    except Exception as e:
        print(f"Error in attribute_conn_db: {e}")
    try:
        print("Start attribute_value_conn_db")
        attribute_value_conn_db()
        print("Finish attribute_value_conn_db")
    except Exception as e:
        print(f"Error in attribute_value_conn_db: {e}")
    try:
        print("Start default_value_conn_db")
        default_value_conn_db()
        print("Finish default_value_conn_db")
    except Exception as e:
        print(f"Error in default_value_conn_db: {e}")

    # formula
    try:
        print("Start searchFormula")
        searchFormula()
        print("Finish searchFormula")
    except Exception as e:
        print(f"Error in searchFormula: {e}")
    try:
        print("Start product_pricelist_item")
        product_pricelist_item()
        print("Finish product_pricelist_item")
    except Exception as e:
        print(f"Error in product_pricelist_item: {e}")
    try:
        print("Start product_pricelist_type")
        product_pricelist_type()
        print("Finish product_pricelist_type")
    except Exception as e:
        print(f"Error in product_pricelist_type: {e}")
    try:
        print("Start configurable_variant_value")
        configurable_variant_value()
        print("Finish configurable_variant_value")
    except Exception as e:
        print(f"Error in configurable_variant_value: {e}")

    # New
    try:
        print("Group chapklang Processing....")
        group_chapklang()
        print("Group chapklang Finish.")
    except Exception as e:
        print(f"Error Group chapklang {e}")
    try:
        print("Rate chapklang Processing....")
        rate_chapklang()
        print("Rate chapklang Finish.")
    except Exception as e:
        print(f"Error chapklang {e}")
    try:
        print("Price chapklang Processing....")
        price_chapklang()
        print("Price chapklang Finish.")
    except Exception as e:
        print(f"Error Price chapklang {e}")
    try:
        print("Price switch Processing....")
        price_switch()
        print("Price switch Finish.")
    except Exception as e:
        print(f"Error Price switch {e}")
    try:
        print("Price remote Processing....")
        price_remote()
        print("Price remote Finish.")
    except Exception as e:
        print(f"Error Price remote {e}")
    # try:
    #     print("productCategory Processing....")
    #     product_category()
    #     print("productCategory Finish.")
    # except Exception as e:
    #     print(f"Error productCategory {e}")


def quit_program():
    time.sleep(2)
    os._exit(0)


if __name__ == "__main__":
    print('=============== START PROGRAM =================')
    thread = Thread(target=process)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    quit_program()
