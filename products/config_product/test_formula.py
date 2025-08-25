def test_manmuan_min():
    # ม่านม้วน ขั้นต่ำ
    area = 0
    width = 0.5
    height = 0.5
    pricelist_price = 596
    code_py1 = """area = width * height\narea_y = area * 1.2\n\nif area < 1 :\n  quantity = area_y\n  addon = (1.2 - area_y) * pricelist_price\n  value = pricelist_price\nelse :\n  quantity = area_y\n  value = pricelist_price\n  \n"""

    # สร้าง namespace เพื่อใช้กับ exec()
    local_vars = {"width": width, "height": height, "pricelist_price": pricelist_price}
    exec(code_py1, {}, local_vars)

    # ดึงค่าที่ต้องการจาก local_vars
    area = local_vars.get("area")
    quantity = local_vars.get("quantity")
    addon = local_vars.get("addon", None)  # addon อาจไม่มีในบางกรณี
    value = local_vars.get("value")

    # แสดงผล
    print(f"Area: {area}")
    print(f"Quantity: {quantity}")
    print(f"Addon: {addon if addon is not None else 'N/A'}")
    print(f"Value: {value}")


# def test_manmuan_min():
#     # ม่านม้วน ขั้นต่ำ
#     area = 0
#     width = 0.5
#     height = 0.5
#     pricelist_price = 596
#     code_py1 = """area = width * height\narea_y = area * 1.2\n\nif area < 1 :\n  quantity = area_y\n  addon = (1.2 - area_y) * pricelist_price\n  value = pricelist_price\nelse :\n  quantity = area_y\n  value = pricelist_price\n  \n"""
#
#     # สร้าง namespace เพื่อใช้กับ exec()
#     local_vars = {"width": width, "height": height, "pricelist_price": pricelist_price}
#     exec(code_py1, {}, local_vars)
#
#     # ดึงค่าที่ต้องการจาก local_vars
#     area = local_vars.get("area")
#     quantity = local_vars.get("quantity")
#     addon = local_vars.get("addon", None)  # addon อาจไม่มีในบางกรณี
#     value = local_vars.get("value")
#
#     # แสดงผล
#     print(f"Area: {area}")
#     print(f"Quantity: {quantity}")
#     print(f"Addon: {addon if addon is not None else 'N/A'}")
#     print(f"Value: {value}")


test_manmuan_min()




