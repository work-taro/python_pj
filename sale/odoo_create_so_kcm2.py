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

data = pd.read_excel(r"C:\Users\kc\Desktop\kcm2.xlsx", skiprows=5)
df = pd.DataFrame(data, columns=["SKU", "จำนวน", "ราคาสินค้า/ชิ้น"])
# df_to_string_sku = df["SKU"].to_string(index=False).strip()
# df_to_array_sku = df_to_string_sku.strip().split()
# print(df_to_array_sku)


products = []
for index, row in df.iterrows():
    product_id = row['SKU']
    quantity = row['จำนวน']
    price_unit = row['ราคาสินค้า/ชิ้น']
    product_info = {
        'product_id': str(product_id),  # แปลงเป็นสตริงในกรณีที่ต้องการ
        'product_uom_qty': int(quantity),  # แปลงเป็นจำนวนเต็ม
        'price_unit': float(price_unit)  # แปลงเป็นทศนิยม
    }
    products.append(product_info)

# pper
# source = './../../Files/Product-pos.xlsx'
# data = pd.read_excel(source, skiprows=5)
#
# # แปลงข้อมูลเป็นรูปแบบที่ต้องการ
# formatted_data = [
#     {
#         'product_id': str(row["SKU"]),
#         'product_uom_qty': row["จำนวน"],
#         'price_unit': row["ราคาสินค้า/ชิ้น"],
#     }
#     for _, row in data.iterrows()
# ]
# print(df_to_string)
val = [
    {
        'cusnam': "6-02-003",
        'type_id': "Normal Order",
        'order_date': "01/02/2024 14:45:56",
        'delivery_date': "01/02/2024",
        'note': "POS-KACEEMALL2 31/JAN/2024",
        'product': products
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
        'warehouse_id': 44,
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
            print("Created SO&Product is Success with ID:", so_name, ' and ', so_product_id)
            print("***************************************")
        else:
            print("Created SO Error")
    except Exception as err:
        print(err)


if __name__ == '__main__':
    search(val)
    # state()