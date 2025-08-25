from threading import Thread
import datetime as dt
import pandas as pd
import time
import dbf
import sys
import re

# --- Config ---
dbf_file_aptrn = "C:/Users/kc/Desktop/DBF/APTRN.DBF"
dbf_file_artrn = "C:/Users/kc/Desktop/DBF/ARTRN.DBF"
dbf_file_aprcpit = "C:/Users/kc/Desktop/DBF/APRCPIT.DBF"
dbf_file_bkmas = "C:/Users/kc/Desktop/DBF/BKMAS.DBF"
dbf_file_bktrn = "C:/Users/kc/Desktop/DBF/BKTRN.DBF"

# วันที่เริ่มฟิลเตอร์
start_date = dt.date(2025, 7, 1)


def read_dbf(path, codepage="cp874"):
    table = dbf.Table(path, codepage=codepage)
    table.open(mode=dbf.READ_ONLY)

    field_names = table.field_names  # list ของชื่อฟิลด์
    records = []
    for rec in table:
        rec_dict = {field: rec[field] for field in field_names}
        records.append(rec_dict)

    table.close()
    return pd.DataFrame(records)


def processAPTRN():
    print('PROCESS APTRN.....')
    try:

        # --- อ่าน APTRN ---
        aptrn_df = read_dbf(dbf_file_aptrn)

        # filter: DOCNUM prefix + DOCDAT >= start_date
        aptrn_df["DOCDAT"] = pd.to_datetime(aptrn_df["DOCDAT"], errors="coerce")
        aptrn_filtered = aptrn_df[
            aptrn_df["DOCNUM"].str.startswith(("HI", "HP", "PS", "PN")) &
            (aptrn_df["DOCDAT"].dt.date >= start_date)
            ]
        return aptrn_filtered

    except Exception as e:
        print(f"Error APTRN {e}")


def processARTRN():
    print('PROCESS ARTRN.....')
    try:
        # --- อ่าน ARTRN ---
        artrn_df = read_dbf(dbf_file_artrn)
        artrn_df["DOCDAT"] = pd.to_datetime(artrn_df["DOCDAT"], errors="coerce")

        # filter: DOCTYPE + DOCDAT >= start_date
        artrn_filtered = artrn_df[
            artrn_df["DOCNUM"].str.startswith(("RE", "RC", "RD", "RM", "AI", "AC")) &
            (artrn_df["DOCDAT"].dt.date >= start_date)
            ]

        return artrn_filtered
    except Exception as e:
        print(f"Error ARTRN {e}")


def processAPRCPIT():
    print('PROCESS APRCPIT.....')
    try:
        # --- อ่าน ARTRN ---
        aprcpit_df = read_dbf(dbf_file_aprcpit)

        # filter: DOCTYPE + DOCDAT >= start_date
        aprcpit_filtered = aprcpit_df[
            aprcpit_df["RCPNUM"].str.startswith(("HI", "HP", "PS", "PN"))
            ]

        return aprcpit_filtered
    except Exception as e:
        print(f"Error APRCPIT {e}")


def processBKMAS():
    print('PROCESS BKMAS.....')
    try:
        # --- อ่าน ARTRN ---
        bkmas_df = read_dbf(dbf_file_bkmas)

        return bkmas_df
    except Exception as e:
        print(f"Error BKMAS {e}")


def processBKTRN():
    print('PROCESS BKTRN.....')
    try:
        # --- อ่าน ARTRN ---
        bkmas_df = read_dbf(dbf_file_bktrn)
        bkmas_filtered = bkmas_df[
            (bkmas_df["TRNDAT"].dt.date >= start_date)
            ]

        return bkmas_filtered
    except Exception as e:
        print(f"Error BKTRN {e}")


def clean_excel_string(value):
    if isinstance(value, str):
        # ลบตัวอักษร ASCII ควบคุม (0–31) ยกเว้น \t, \n, \r
        return re.sub(r"[\x00-\x08\x0B-\x1F\x7F]", "", value)
    return value


def exportToExcel(bktrnResult):
    print('Export Excel.....')

    # ลบ illegal characters ในทุก column
    # aptrnResult = aptrnResult.applymap(clean_excel_string)
    # artrnResult = artrnResult.applymap(clean_excel_string)
    # aprcpitResult = aprcpitResult.applymap(clean_excel_string)
    # bkmasResult = bkmasResult.applymap(clean_excel_string)
    bktrnResult = bktrnResult.applymap(clean_excel_string)
    # aptrnResult.to_excel("C:/Users/kc/Desktop/DBF/report/AP_ARTRN/APTRN_process.xlsx", index=False)
    # artrnResult.to_excel("C:/Users/kc/Desktop/DBF/report/AP_ARTRN/ARTRN_process.xlsx", index=False)
    # aprcpitResult.to_excel("C:/Users/kc/Desktop/DBF/report/AP_ARTRN/APRCPIT_process.xlsx", index=False)
    bktrnResult.to_excel("C:/Users/kc/Desktop/DBF/report/AP_ARTRN/BKTRN_process.xlsx", index=False)

    # print("✅ Exported APTRN_filtered.xlsx")
    # print("✅ Exported ARTRN_filtered.xlsx")


def quit_program():
    time.sleep(1)
    sys.exit()


def task():
    ts = time.time()
    # aptrnResult = processAPTRN()
    # artrnResult = processARTRN()
    # aprcpitResult = processAPRCPIT()
    # bkmasResult = processBKMAS()
    bktrnResult = processBKTRN()
    # exportToExcel(aptrnResult, artrnResult)


    exportToExcel(bktrnResult)
    print(f'================ TOTAL TIME OF PROCESS {time.time() - ts} SECOND ================')


if __name__ == "__main__":
    print('=============== START PROGRAM =================')
    thread = Thread(target=task)
    thread.start()
    thread.join()
    print('================ END PROGRAM ==================')
    quit_program()


