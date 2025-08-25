import os
import sys
import time
import requests
from datetime import datetime
import xmlrpc.client
import pandas as pd
from threading import Thread
import random

baseURL = ("https://www.bot.or.th/content/bot/th/statistics/exchange-rate/jcr:content/root/container/statisticstable2"
           ".results.level3cache.json")
baseURLmonth = ("https://www.bot.or.th/content/bot/th/statistics/exchange-rate/jcr:content/root/container"
                "/statisticstable1.results.level3cache.daily.2567-05-28.2567-05-29.USD$MYR$CNY.json")

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:85.0) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:78.0) Gecko/20100101 Firefox/78.0"
]


thai_to_eng_month = {
    'ม.ค.': 'Jan',
    'ก.พ.': 'Feb',
    'มี.ค.': 'Mar',
    'เม.ย.': 'Apr',
    'พ.ค.': 'May',
    'มิ.ย.': 'Jun',
    'ก.ค.': 'Jul',
    'ส.ค.': 'Aug',
    'ก.ย.': 'Sep',
    'ต.ค.': 'Oct',
    'พ.ย.': 'Nov',
    'ธ.ค.': 'Dec'
}


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

urlKC = config.get('url')
dbKC = config.get('db')
usernameKC = config.get('username')
passwordKC = config.get('password')
moduleKC = config.get('module')

urlEV = config.get('url_ev')
dbEV = config.get('db_ev')
usernameEV = config.get('username_ev')
passwordEV = config.get('password_ev')
moduleEV = config.get('module_ev')


def convert_thai_date_to_iso(thai_date):
    # แปลงวันที่ไทยเป็นวันที่ ISO
    day, month, year_th = thai_date.split(' ')
    year = int(year_th) - 543  # แปลงปีพุทธศักราชเป็นคริสต์ศักราช
    month = thai_to_eng_month[month]  # แปลงชื่อเดือน
    date_str = f"{day} {month} {year}"
    date_obj = datetime.strptime(date_str, "%d %b %Y")
    return date_obj.strftime("%Y-%m-%d")


def getData():
    headers = {'User-Agent': random.choice(user_agents)}
    response = requests.get(baseURL, headers=headers)
    # response = requests.get(baseURLmonth, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Filter the data for specific currencies
        currencies = ["USD", "MYR", "CNY"]
        filtered_data = [item for item in data['responseContent'] if item['currency_id'] in currencies]

        # # Convert the date format
        # for item in filtered_data:
        #     item['period'] = convert_thai_date_to_iso(item['period'])

        return filtered_data
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
        return None


def connectOdoo(url, db, username, password):
    # สร้าง ServerProxy สำหรับเชื่อมต่อกับ Odoo
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    if uid:
        return models, uid
    else:
        return None, None


def uploadToOdoo(url, db, username, password, module):
    data = getData()
    if data is None:
        print(f"No data to upload for module {module}.")
        return

    models, uid = connectOdoo(url, db, username, password)
    if models and uid:
        df = pd.DataFrame(data)
        currency_id = 0
        for index, row in df.iterrows():
            if row['currency_id'] == 'USD':
                currency_id = 2
            elif row['currency_id'] == 'CNY':
                currency_id = 7
            elif row['currency_id'] == 'MYR':
                currency_id = 34

            # Prepare the data to upload
            data_to_upload = {
                'name': row['period'],
                'currency_id': currency_id,
                'inverse_company_rate': row['selling'],
            }

            try:
                models.execute_kw(db, uid, password, 'res.currency.rate', 'create', [data_to_upload])
                print(f"Completed to Uploaded data {module} for {row['currency_id']}: {row['period']}")
            except Exception as e:
                print(f"Failed to upload data {module} for {row['currency_id']}: {row['period']} => {e}")


def quit_program():
    time.sleep(2)
    os._exit(0)


if __name__ == "__main__":
    print('=============== START PROGRAM =================')

    # Start thread for the first Odoo instance
    print('Currencies KC : Starting...')
    thread1 = Thread(target=uploadToOdoo, args=(urlKC, dbKC, usernameKC, passwordKC, moduleKC))
    thread1.start()
    thread1.join()

    # Start thread for the second Odoo instance
    print('Currencies EV : Starting...')
    thread2 = Thread(target=uploadToOdoo, args=(urlEV, dbEV, usernameEV, passwordEV, moduleEV))
    thread2.start()
    thread2.join()

    print('================ END PROGRAM ==================')
    quit_program()

