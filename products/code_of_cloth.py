import pandas as pd
import re


def one():
    # อ่านข้อมูลจาก Excel
    data = pd.read_excel('C:/Users/kc/Desktop/ประเภทกลุ่มสินค้าผ้าม่าน_edit.xlsx', skiprows=1)

    # สร้าง DataFrame
    df = pd.DataFrame(data)

    # ใช้ regex เพื่อดึงรหัสจาก column 'รายละเอียด' และสร้าง column 'F'
    # ดึงตัวอักษร, ตัวเลข, และขีด (-) ที่ต่อกัน
    df['F'] = df['รายละเอียด'].str.extract(r'([A-Za-z0-9\s\-]+(?:\s?[0-9]+-[0-9]+)?)')

    # ลบคำที่ไม่ต้องการ
    df['F'] = df['F'].str.replace(r'Black\s?Out|BlackOut|BLACKt|DIM OUT|Black out|Blackout|Dim Out|BLACKOUT|BlackOut-|Dim out ', '', regex=True)

    # ลบช่องว่างจากตำแหน่งแรก (ซ้ายสุด)
    df['F'] = df['F'].str.lstrip()

    # แสดงผลลัพธ์
    print(df)

    # บันทึกไฟล์ Excel พร้อมคอลัมน์ใหม่
    df.to_excel('C:/Users/kc/Desktop/ประเภทกลุ่มสินค้าผ้าม่าน_edit_updated_test.xlsx', index=False)



def two():
    file_path = 'C:/Users/kc/Desktop/ประเภทกลุ่มสินค้าผ้าม่าน_edit.xlsx'
    data = pd.read_excel(file_path, skiprows=1)

    # Copy the original DataFrame for modification
    df = data.copy()

    # ฟังก์ชันใหม่สำหรับประมวลผลข้อมูลในคอลัมน์ 'รายละเอียด'
    def process_detail(detail):
        if isinstance(detail, str):
            # กรณีที่มีคำว่า 'พลาสติก' ให้คืนค่าเป็นค่าว่าง
            if 'พลาสติก' in detail:
                return ''
            # จับข้อความรูปแบบเช่น "ROMA 4A" หรือ "DX084B-W50"
            match = re.search(r'\b[A-Z]+ ?[A-Z0-9\-]*\b', detail)
            if match:
                return match.group(0)
        return detail  # คืนค่าข้อความต้นฉบับถ้าไม่ตรงเงื่อนไขใด

    # ใช้ฟังก์ชันกับคอลัมน์ 'รายละเอียด'
    df['F'] = df['รายละเอียด'].apply(process_detail)

    # Save the updated DataFrame to a new Excel file
    output_path = 'C:/Users/kc/Desktop/ประเภทกลุ่มสินค้าผ้าม่าน_uppp.xlsx'
    df.to_excel(output_path, index=False)

    print(f"Processed file has been saved to: {output_path}")


one()
# two()
