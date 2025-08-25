import pandas as pd
import xmlrpc.client

url = 'http://192.168.9.102:8069'
db = 'kc_uat03'
username = '660100'
password = 'Kacee2023'
# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

data = pd.read_excel(r"C:\Users\kc\Desktop\Stock_Odoo_1698891903630.xlsx")
df = pd.DataFrame(data, columns=["SKU", "ชื่อสินค้า"])
df_to_string = df[["SKU", "ชื่อสินค้า"]].to_string(index=False).strip()
df_to_array = df_to_string.strip().split('\n')


print(df)
print(df_to_string)
print(df_to_array)
print(len(df_to_array))


# update data by sku
def update_data():
    id_records = []

    for sku in df_to_array:
        sku = sku.strip()  # Remove leading and trailing spaces
        print(sku)
        condition = [('default_code', 'ilike', sku)]
        users = models.execute_kw(db, uid, password, 'product.template', 'search_read', [condition], {'fields': ['default_code', 'route_ids']})
        id_list = [user['id'] for user in users]
        id_records.append(id_list)
        for user in users:
            print(user)

    print(id_records)
    # updated_data = {
    #     'route_ids': [5, 144],
    # }
    # id_records = [item for sublist in id_records for item in sublist]
    # result = models.execute_kw(db, uid, password,
    #                            'product.template', 'write',
    #                            [id_records, updated_data])
    #
    # if result:
    #     print("Records updated successfully.")
    # else:
    #     print("Failed to update records.")


# update_data()

