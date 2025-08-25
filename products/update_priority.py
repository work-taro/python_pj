import pandas as pd
import xmlrpc.client

# KC
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# EV
# url = 'http://192.168.9.101:8069/'
# db = 'ev_uat02'
# username = '660100'
# password = 'Kacee2023'

# Authenticate
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)

data = pd.read_excel(r"C:\Users\kc\Desktop\product.template edit.xlsx")
df = pd.DataFrame(data, columns=["ID", "Priority"])

df_to_string = df["ID"].to_string(index=False).strip()
df_to_array = df_to_string.strip().split('\n')

df_to_string_priority = df["Priority"].to_string(index=False).strip()
df_to_array_priority = df_to_string_priority.strip().split('\n')

# print(df)
# print(df_to_string)
# print(df_to_array)
# print(len(df_to_array))
# print(df_to_string_priority)
# print(df_to_array_priority)
# print(len(df_to_array_priority))


# def update_data():
#     id_records = []
#
#     for name in df_to_array:
#         name = name.strip()  # Remove leading and trailing spaces
#         print(name)
#         condition = [('name', 'ilike', name)]
#         users = models.execute_kw(db, uid, password, 'product.template', 'search_read', [condition], {'fields': ['name', 'priority']})
#         id_list = [user['id'] for user in users]
#         id_records.append(id_list)
#         for user in users:
#             print(user)
#
#     print(id_records)
#     print(len(id_records))
#     updated_data = {
#         'priority': [5, 144],
#     }
#     id_records = [item for sublist in id_records for item in sublist]
#     result = models.execute_kw(db, uid, password,
#                                'product.template', 'write',
#                                [id_records, updated_data])
#
#     if result:
#         print("Records updated successfully.")
#     else:
#         print("Failed to update records.")

def update_data():
    name_to_priority = dict(zip(df_to_array, df_to_array_priority))
    # print(name_to_priority)

    for name, priority in name_to_priority.items():
        name = name.strip()  # Remove leading and trailing spaces
        priority = int(priority.strip())  # Convert priority to integer
        condition = [('id', 'ilike', name)]
        products = models.execute_kw(db, uid, password, 'product.template', 'search_read', [condition],
                                     {'fields': ['id', 'priority']})
        # print(name)
        # print(products)

        for product in products:
            product_id = product['id']
            updated_data = {'priority': priority}
            result = models.execute_kw(db, uid, password, 'product.template', 'write', [[product_id], updated_data])

            if result:
                print(f"Priority for product '{name}' updated successfully to '{priority}'.")
            else:
                print(f"Failed to update priority for product '{name}'.")


update_data()
