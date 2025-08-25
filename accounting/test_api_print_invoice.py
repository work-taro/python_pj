import xmlrpc.client
import base64

# ข้อมูลการเชื่อมต่อ
url = 'http://192.168.9.102:8069'
db = 'KC_UAT_27022024'
username = '660100'
password = 'Kacee2023'

# การเชื่อมต่อ Common API
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
if not uid:
    raise Exception("Authentication failed. Please check your credentials.")

# การเชื่อมต่อ Object API
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

# ID ของ Invoice ที่ต้องการปริ้น (เปลี่ยน 1166 เป็น ID ที่ต้องการ)
invoice_id = 1166

# ชื่อรายงาน (มาตรฐาน Odoo สำหรับ Invoice)
report_name = 'account.report_invoice'


def call_report():
    try:
        result = models.execute_kw(
            db, uid, password,
            'ir.actions.report', 'report_download',
            [report_name, [invoice_id]]
        )

        # บันทึก PDF ลงไฟล์
        pdf_content = base64.b64decode(result[0])  # ถอดรหัสเนื้อหา PDF
        with open("invoice_1166.pdf", "wb") as f:
            f.write(pdf_content)

        print("PDF ถูกสร้างสำเร็จ: invoice_1166.pdf")
    except xmlrpc.client.Fault as e:
        print(f"เกิดข้อผิดพลาด: {e}")


def check_report():
    reports = models.execute_kw(
        db, uid, password,
        'ir.actions.report', 'search_read',
        [[['report_type', '=', 'qweb-pdf']]],
        {'fields': ['id', 'name', 'report_name', 'report_type']}
    )

    for report in reports:
        print(
            f"ID: {report['id']}, Name: {report['name']}, Report Name: {report['report_name']}, Type: {report['report_type']}")


call_report()
# check_report()