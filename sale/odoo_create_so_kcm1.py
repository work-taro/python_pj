import xmlrpc.client
from datetime import datetime, timedelta
import time
import pandas as pd

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT26122023'
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

data_1 = pd.read_excel(r"C:\Users\kc\Desktop\no1.xlsx", skiprows=5)
df_1 = pd.DataFrame(data_1, columns=["SKU", "จำนวน", "ราคาสินค้า/ชิ้น"])
# df_to_string_sku = df["SKU"].to_string(index=False).strip()
# df_to_array_sku = df_to_string_sku.strip().split()
# print(df_to_array_sku)
data_2 = pd.read_excel(r"C:\Users\kc\Desktop\no2.xlsx", skiprows=5)
df_2 = pd.DataFrame(data_2, columns=["SKU", "จำนวน", "ราคาสินค้า/ชิ้น"])

data_3 = pd.read_excel(r"C:\Users\kc\Desktop\no3.xlsx", skiprows=5)
df_3 = pd.DataFrame(data_3, columns=["SKU", "จำนวน", "ราคาสินค้า/ชิ้น"])

data_self_checkout = pd.read_excel(r"C:\Users\kc\Desktop\self_checkout.xlsx", skiprows=5)
df_self_checkout = pd.DataFrame(data_self_checkout, columns=["SKU", "จำนวน", "ราคาสินค้า/ชิ้น"])


products_no1 = []
for index, row in df_1.iterrows():
    product_id_1 = row['SKU']
    quantity_1 = row['จำนวน']
    price_unit_1 = row['ราคาสินค้า/ชิ้น']
    product_info_1 = {
        'product_id': str(product_id_1),  # แปลงเป็นสตริงในกรณีที่ต้องการ
        'product_uom_qty': int(quantity_1),  # แปลงเป็นจำนวนเต็ม
        'price_unit': float(price_unit_1)  # แปลงเป็นทศนิยม
    }
    products_no1.append(product_info_1)

products_no2 = []
for index, row in df_2.iterrows():
    product_id_2 = row['SKU']
    quantity_2 = row['จำนวน']
    price_unit_2 = row['ราคาสินค้า/ชิ้น']
    product_info_2 = {
        'product_id': str(product_id_2),  # แปลงเป็นสตริงในกรณีที่ต้องการ
        'product_uom_qty': int(quantity_2),  # แปลงเป็นจำนวนเต็ม
        'price_unit': float(price_unit_2)  # แปลงเป็นทศนิยม
    }
    products_no2.append(product_info_2)

products_no3 = []
for index, row in df_3.iterrows():
    product_id_3 = row['SKU']
    quantity_3 = row['จำนวน']
    price_unit_3 = row['ราคาสินค้า/ชิ้น']
    product_info_3 = {
        'product_id': str(product_id_3),  # แปลงเป็นสตริงในกรณีที่ต้องการ
        'product_uom_qty': int(quantity_3),  # แปลงเป็นจำนวนเต็ม
        'price_unit': float(price_unit_3)  # แปลงเป็นทศนิยม
    }
    products_no3.append(product_info_3)

products_self_checkout = []
for index, row in df_self_checkout.iterrows():
    product_id_self_checkout = row['SKU']
    quantity_self_checkout = row['จำนวน']
    price_unit_self_checkout = row['ราคาสินค้า/ชิ้น']
    product_info_self_checkout = {
        'product_id': str(product_id_self_checkout),  # แปลงเป็นสตริงในกรณีที่ต้องการ
        'product_uom_qty': int(quantity_self_checkout),  # แปลงเป็นจำนวนเต็ม
        'price_unit': float(price_unit_self_checkout)  # แปลงเป็นทศนิยม
    }
    products_self_checkout.append(product_info_self_checkout)

val = [
    {
        # 'cusnam': "6-02-001",   # no1
        # 'cusnam': "6-02-00101",   # no2
        # 'cusnam': "6-02-00102",   # no3
        'cusnam': "6-02-00103",   # self_checkout

        'type_id': "Normal Order",
        'order_date': "19/03/2024 14:51:35",
        'delivery_date': "19/03/2024",

        # 'note': "POS-KACEEMALL1 เครื่อง1 @31/JAN/2024",
        # 'note': "POS-KACEEMALL1 เครื่อง2 @31/JAN/2024",
        # 'note': "POS-KACEEMALL1 เครื่อง3 @31/JAN/2024",
        'note': "POS-KACEEMALL1 self_checkout @18/JAN/2024 ",
        'status': "sale",

        # 'product': products_no1,
        # 'product': products_no2,
        # 'product': products_no3,
        'product': products_self_checkout,
    },
    # {
    #         'cusnam': "3-03-002",
    #         'type_id': "Sale Online",
    #         'order_date': "31/01/2024 16:33:59",
    #         'delivery_date': "31/01/2024",
    #         'note': "shopee",
    #         'product': [
    #             {
    #                 'product_id': 'B05-1205208',
    #                 'product_uom_qty': 10,
    #                 'price_unit': 899,
    #             },
    #         ],
    #     },
]


def search(val):
    table = 'res.partner'
    table2 = 'sale.order.type'

    for row in val:
        where = [('ref', '=', row['cusnam'])]
        datas = models.execute_kw(db, uid, password, table, 'search_read', [where], {'fields': ['id', ]})

        where2 = [('name', '=', row['type_id'])]
        datas2 = models.execute_kw(db, uid, password, table2, 'search_read', [where2],
                                   {'fields': ['id', 'warehouse_id']})

        # where3 = [('name', '=', datas2[0]['warehouse_id'])]
        # datas3 = models.execute_kw(db, uid, password, 'stock.warehouse', 'search_read', [where3], {'fields': ['id']})

        cusid = datas[0]['id']
        typeId = datas2[0]['id']
        whId = 44

        create(row, cusid, typeId)
        time.sleep(1)


def create(row, cusid, typeId):
    table = 'product.product'
    _date = datetime.strptime(row['delivery_date'], "%d/%m/%Y")
    _datetime = datetime.strptime(row['order_date'], "%d/%m/%Y %H:%M:%S")
    order_date = datetime.strptime(row['order_date'], "%d/%m/%Y %H:%M:%S") - timedelta(hours=7)
    od = order_date.strftime("%Y-%m-%d %H:%M:%S")

    data = {
        'partner_id': cusid,
        'user_id': 2129,
        'date_order': od,
        'pricelist_id': 1,
        'type_id': typeId,
        'delivery_date': _date.strftime("%Y-%m-%d"),
        'note': row['note'],
        'warehouse_id': 43,
        'payment_check': 13,
    }

    try:
        so_id = models.execute_kw(db, uid, password, 'sale.order', 'create', [data])

        where = [('id', '=', so_id)]
        so_datas = models.execute_kw(db, uid, password, 'sale.order', 'search_read', [where], {'fields': ['name']})
        so_name = so_datas[0]['name']

        if isinstance(so_id, int):
            for i in row['product']:
                where = [('default_code', '=', i['product_id'])]
                datas = models.execute_kw(db, uid, password, table, 'search_read', [where], {'fields': ['id']})
                if len(datas) > 0:
                    i['product_id'] = datas[0]['id']
                i['order_id'] = so_id

            product = row['product']
            so_product_id = models.execute_kw(db, uid, password, 'sale.order.line', 'create', [product])

            where = [('name', '=', so_name)]
            datas = models.execute_kw(db, uid, password, "sale.order", 'search_read', [where],
                                      {'fields': ['id', 'name', 'state']})
            for data in datas:
                print(data)
                for so in datas:
                    so_id = so['id']
                    updated_data = {'state': 'sale'}
                    result = models.execute_kw(db, uid, password, 'sale.order', 'write', [[so_id], updated_data])

                    if result:
                        print(f"Status for SO '{so_id}' updated successfully to {updated_data['state']}")
                    else:
                        print(f"Failed to update priority for product '{so_id}'.")
            print("Created SO&Product is Success with ID:", so_name, ' and ', so_product_id)
            print("***************************************")
        else:
            print("Created SO Error")
    except Exception as err:
        print(err)


if __name__ == '__main__':
    search(val)
    # state()