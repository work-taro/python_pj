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
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

if uid:
    print("Connected successfully!", uid)
else:
    print("Connection failed!", uid)


def searchDefault():
    product_condition = [('id', '=', 252746)]
    product_template = models.execute_kw(db, uid, password,
                                              'product.template', 'search_read',
                                              [product_condition], {
                                                  'fields': []})
    for product in product_template:
        print(product)
        size_based_on_id = product['size_based_on'][0] if isinstance(product['size_based_on'], list) else None
        size_based_on_name = product['size_based_on'][1] if isinstance(product['size_based_on'], list) else None
        print(size_based_on_id, size_based_on_name)


def searchVariantValue():
    product_condition = [('id', '=', 791460)]
    product_template = models.execute_kw(db, uid, password,
                                              'product.template.attribute.value', 'search_read',
                                              [product_condition], {
                                                  'fields': []})
    for product in product_template:
        print(product)
        # size_based_on_id = product['size_based_on'][0] if isinstance(product['size_based_on'], list) else None
        # size_based_on_name = product['size_based_on'][1] if isinstance(product['size_based_on'], list) else None
        # print(size_based_on_id, size_based_on_name)


def searchPricelist():
    product_condition = [('id', '=', 7)]
    product_template = models.execute_kw(db, uid, password,
                                              'product.pricelist', 'search_read',
                                              [product_condition], {
                                                  'fields': []})
    for product in product_template:
        print(product)


def searchFormula():
    product_condition = [('id', '=', 10)]
    product_template = models.execute_kw(db, uid, password,
                                              'pricing.formula', 'search_read',
                                              [product_condition], {
                                                  'fields': []})
    for product in product_template:
        print(product)


def search_attribute_value_related_product():
    condition = [('id', '=', 31693)]
    attr_value_tem = models.execute_kw(db, uid, password, 'product.attribute.value', 'search_read', [condition], {
        'fields': []})
    for a in attr_value_tem:
        print(a)
        multi_combo_id = a.get('related_product_id', None)  # ถ้าไม่มี key ให้เป็น None
        if isinstance(multi_combo_id, list):  # ถ้าเป็น list
            if multi_combo_id:  # ถ้า list ไม่ว่าง
                multi_combo_id = multi_combo_id[0]  # ใช้ค่าแรกใน array
            else:
                multi_combo_id = None
        condition_s = [('id', '=', multi_combo_id)]
        attr_value_tem_s = models.execute_kw(db, uid, password, 'multi.combo.option', 'search_read', [condition_s], {
            'fields': []})
        for b in attr_value_tem_s:
            print(b)
            product_variants_id = b['product_id'][0]
            print(product_variants_id)
            condition_product_template = [('id', '=', product_variants_id)]
            pro_tem = models.execute_kw(db, uid, password, 'product.product', 'search_read', [condition_product_template],
                                                 {
                                                     'fields': []})
            for p in pro_tem:
                related_product_id = p['product_tmpl_id'][0]
                print(p)
                print(related_product_id)



def product_pricelist_item():
        product_template_condition = [('id', '=', 285613)]
        product_template = models.execute_kw(db, uid, password,
                                             'product.pricelist.item', 'search_read',
                                             [product_template_condition],
                                             {'fields': []})
        for product in product_template:
            print(product)
            fixed_price = product['fixed_price']
            print(fixed_price)

def muli_test():
    width = 2
    height = 2
    price_table = {
        0.50: [930, 1040, 1110, 1290, 1390, 1460, 1540, 1620, 1780, 1870, 1940, 2040, 2070, 2130, 2220, 2240, 2270,
               2350, 2520, 2560, 2720, 2750, 2840, 2940, 3060, 3090, 3390, 3460, 3580, 3680, 3800, 3920, 4040],
        0.60: [1040, 1130, 1220, 1390, 1460, 1540, 1640, 1780, 1930, 2040, 2110, 2190, 2240, 2280, 2400, 2420, 2460,
               2560, 2720, 2780, 2920, 3010, 3070, 3240, 3330, 3390, 3680, 3720, 3840, 3950, 4090, 4220, 4350],
        0.70: [1110, 1240, 1290, 1470, 1600, 1710, 1800, 1890, 2110, 2160, 2280, 2400, 2410, 2520, 2590, 2650, 2710,
               2800, 2950, 3060, 3210, 3280, 3390, 3530, 3620, 3690, 3980, 4060, 4190, 4330, 4460, 4600, 4740],
        0.80: [1180, 1290, 1410, 1600, 1720, 1820, 1940, 2060, 2240, 2350, 2520, 2600, 2650, 2690, 2780, 2860, 2890,
               3000, 3160, 3240, 3420, 3530, 3600, 3780, 3890, 3920, 4270, 4360, 4520, 4650, 4800, 4940, 5110],
        0.90: [1240, 1350, 1460, 1710, 1870, 1980, 2070, 2150, 2350, 2520, 2600, 2750, 2810, 2870, 2940, 3050, 3070,
               3220, 3360, 3510, 3660, 3760, 3870, 4040, 4150, 4210, 4540, 4660, 4810, 4980, 5120, 5280, 5450],
        1.00: [1280, 1390, 1510, 1740, 1890, 2060, 2130, 2270, 2520, 2600, 2780, 2920, 2930, 3070, 3180, 3240, 3330,
               3400, 3620, 3720, 3880, 4050, 4210, 4310, 4400, 4480, 4860, 4980, 5130, 5290, 5460, 5640, 5810],
        1.10: [1340, 1450, 1600, 1810, 1950, 2110, 2240, 2350, 2580, 2740, 2940, 3070, 3110, 3220, 3390, 3560, 3620,
               3720, 3820, 4150, 4190, 4270, 4360, 4540, 4690, 4800, 5150, 5270, 5450, 5600, 5780, 5950, 6150],
        1.20: [1350, 1460, 1620, 1870, 2040, 2130, 2250, 2450, 2640, 2840, 3000, 3130, 3280, 3390, 3560, 3720, 3820,
               3920, 4010, 4360, 4390, 4480, 4620, 4750, 4860, 4990, 5340, 5480, 5660, 5840, 6010, 6210, 6400],
        1.30: [1410, 1520, 1680, 1920, 2090, 2280, 2390, 2560, 2860, 2940, 3130, 3340, 3450, 3540, 3680, 3860, 3940,
               4090, 4240, 4560, 4580, 4690, 4840, 4950, 5060, 5180, 5530, 5680, 5860, 6050, 6240, 6420, 6640],
        1.40: [1460, 1600, 1760, 1990, 2190, 2360, 2520, 2620, 2890, 3070, 3330, 3450, 3600, 3720, 3840, 4050, 4110,
               4240, 4460, 4750, 4810, 4950, 5050, 5160, 5190, 5360, 5720, 5870, 6060, 6250, 6450, 6650, 6860],
        1.50: [1470, 1620, 1800, 2070, 2210, 2450, 2580, 2740, 2920, 3180, 3340, 3520, 3660, 3920, 4050, 4190, 4310,
               4390, 4660, 4950, 5050, 5210, 5270, 5400, 5450, 5480, 5880, 6060, 6250, 6460, 6650, 6860, 7070],
        1.60: [1520, 1680, 1880, 2110, 2280, 2530, 2640, 2810, 3050, 3270, 3480, 3650, 3820, 4070, 4240, 4340, 4420,
               4590, 4860, 5160, 5240, 5420, 5510, 5600, 5750, 5760, 6060, 6220, 6420, 6620, 6820, 7050, 7270],
        1.70: [1580, 1720, 1930, 2210, 2360, 2600, 2740, 2890, 3180, 3390, 3620, 3810, 3990, 4240, 4410, 4510, 4590,
               4800, 5070, 5350, 5480, 5660, 5750, 5870, 5980, 6010, 6330, 6510, 6710, 6920, 7130, 7350, 7590],
        1.80: [1600, 1760, 1940, 2250, 2420, 2690, 2840, 3010, 3240, 3510, 3710, 3880, 4070, 4340, 4520, 4710, 4800,
               5000, 5290, 5540, 5710, 5880, 5990, 6130, 6190, 6270, 6600, 6760, 6990, 7210, 7420, 7660, 7890],
        1.90: [1620, 1780, 2000, 2330, 2470, 2750, 2890, 3090, 3290, 3590, 3780, 3990, 4160, 4420, 4640, 4840, 5000,
               5210, 5510, 5720, 5920, 6130, 6210, 6400, 6460, 6470, 6870, 7040, 7250, 7480, 7720, 7950, 8210],
        2.00: [1680, 1880, 2070, 2360, 2560, 2810, 3000, 3190, 3460, 3690, 3920, 4130, 4330, 4590, 4800, 4980, 5190,
               5390, 5760, 5950, 6160, 6350, 6460, 6620, 6690, 6740, 7130, 7330, 7560, 7800, 8050, 8290, 8540],
        2.10: [1740, 1930, 2130, 2450, 2640, 2890, 3090, 3270, 3540, 3750, 4060, 4270, 4480, 4780, 4990, 5210, 5390,
               5620, 5940, 6160, 6420, 6620, 6690, 6880, 6940, 7000, 7410, 7590, 7840, 8070, 8330, 8590, 8860],
        2.20: [1780, 1940, 2220, 2520, 2720, 3000, 3180, 3360, 3690, 3880, 4210, 4410, 4640, 4940, 5160, 5350, 5620,
               5800, 6150, 6350, 6640, 6880, 6940, 7120, 7210, 7280, 7660, 7880, 8130, 8390, 8650, 8930, 9210],
        2.30: [1840, 2060, 2280, 2590, 2810, 3070, 3250, 3450, 3720, 3940, 4270, 4480, 4710, 5010, 5210, 5480, 5680,
               5920, 6250, 6470, 6740, 6930, 7180, 7360, 7450, 7480, 7930, 8150, 8410, 8680, 8940, 9220, 9520],
        2.40: [1880, 2120, 2350, 2680, 2890, 3120, 3360, 3530, 3780, 4010, 4340, 4590, 4800, 5110, 5330, 5530, 5810,
               5990, 6340, 6580, 6880, 7090, 7290, 7600, 7710, 7760, 8210, 8420, 8690, 8980, 9250, 9530, 9840],
        2.50: [1930, 2150, 2400, 2750, 3010, 3250, 3460, 3650, 3890, 4060, 4410, 4640, 4860, 5130, 5450, 5650, 5860,
               6070, 6450, 6660, 6950, 7180, 7450, 7720, 7940, 7990, 8470, 8710, 8990, 9270, 9560, 9860, 10160],
        2.60: [1980, 2210, 2450, 2810, 3070, 3330, 3580, 3750, 4040, 4190, 4460, 4680, 4890, 5240, 5470, 5690, 5920,
               6150, 6520, 6750, 7050, 7280, 7530, 7810, 8060, 8280, 8750, 8990, 9270, 9560, 9860, 10160, 10480],
        2.70: [2040, 2270, 2560, 2890, 3160, 3410, 3680, 3870, 4150, 4330, 4580, 4720, 4980, 5290, 5530, 5760, 5930,
               6240, 6530, 6880, 7120, 7360, 7600, 7920, 8150, 8390, 9010, 9250, 9540, 9840, 10150, 10470, 10800],
        2.80: [2070, 2310, 2590, 2980, 3240, 3520, 3760, 3990, 4250, 4420, 4710, 4800, 5010, 5340, 5600, 5810, 5990,
               6290, 6740, 6920, 7280, 7450, 7710, 8010, 8240, 8510, 9120, 9360, 9660, 9980, 10280, 10600, 10930],
        2.90: [2130, 2360, 2690, 3060, 3290, 3600, 3870, 4070, 4390, 4560, 4840, 5000, 5180, 5510, 5760, 5990, 6240,
               6470, 6860, 7120, 7420, 7600, 7920, 8250, 8410, 8640, 9360, 9650, 9950, 10270, 10590, 10920, 11270],
        3.00: [2190, 2420, 2750, 3130, 3360, 3690, 4040, 4110, 4510, 4860, 4940, 5010, 5290, 5590, 5880, 6120, 6410,
               6640, 6950, 7280, 7600, 7760, 8090, 8410, 8720, 8890, 9560, 9810, 10120, 10450, 10760, 11110, 11450],
    }

    width_values = [
        0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30, 1.40, 1.50,
        1.60, 1.70, 1.80, 1.90, 2.00, 2.10, 2.20, 2.30, 2.40, 2.50, 2.60,
        2.70, 2.80, 2.90, 3.00, 3.10, 3.20, 3.30, 3.40, 3.50, 3.60, 3.70
    ]

    value = 0

    valid_heights = [h for h in price_table.keys() if h >= height]
    if valid_heights:
        closest_height = min(valid_heights)
    else:
        closest_height = max(price_table.keys())

    # Get the corresponding price list for the closest height
    height_prices = price_table[closest_height]

    # Find the price based on the width
    for i, w in enumerate(width_values):
        if width <= w:
            value = height_prices[i]
            break
    else:
        # If width is larger than the largest value in width_values
        value = height_prices[-1]

    print(value)


# searchDefault()
# searchVariantValue()
# searchPricelist()
# searchFormula()
# search_attribute_value_related_product()
# product_pricelist_item()
muli_test()
