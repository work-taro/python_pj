import requests
import os
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import codecs
from time import sleep
import random
import json


# date = datetime.now().strftime('%Y-%m-%d')
date = '2024-06-11'
base_url = "https://www.kaceebest.com/fn/product_search/?page=placeholder_page"
# base_url = "https://www.kaceebest.com/fn/product_search/?page=1"
web = "kaceebest"

property_type = {
    'all': {'start': 1, 'end': 3}
}

if not os.path.isdir("links/" + date):
    os.mkdir("links/" + date)
path_links = "links/" + date + "/" + web
if not os.path.isdir(path_links):
    os.mkdir(path_links)

if not os.path.isdir('Files/' + date):
    os.mkdir('files/' + date)
path_Files = 'files/' + date + '/' + web
if not os.path.isdir(path_Files):
    os.mkdir(path_Files)

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

link_url = []
ids = []
names = []
skus = []
prices = []
details = []
images = []


def loop_link():
    file_links = codecs.open(path_links + f"/links_all.txt", "r", "utf-8")
    links = file_links.readlines()
    file_links.close()

    ID = 0
    for i in tqdm(range(len(links))):
        ID += 1
        link = links[i]
        get_data(link.strip(), ID)
        sleep(0.3)


def get_data(link, ID):
    headers = {'User-Agent': random.choice(user_agents)}
    res = requests.get(link, headers=headers)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    # print(soup)
    try:
        ids.append(ID)
        # try:
        #     name = soup.find('title').get_text().strip()
        #     names.append(name)
        # except Exception as e:
        #     print('name : ', e)
        #     names.append(None)
        # try:
        #     detail = soup.find('div', id="product-detail").get_text().strip()
        #     details.append(detail)
        # except Exception as e:
        #     print('detail : ', e)
        #     details.append(None)
        # try:
        #     image = soup.find('img', class_='d-block w-100')['src']
        #     img_url = 'www.kaceebest.com' + image
        #     images.append(img_url)
        # except Exception as e:
        #     print('image : ', e)
        #     images.append(None)
        try:
            link_sss = link.replace('//fn', '/fn')
            product_id = link.replace('https://www.kaceebest.com//fn/detail/', '').replace('/', '')
            value = soup.find('span', class_='var-btn').get_text().strip()

            Headers = {
                "Accept": "application/json, text/plain, */*",
                "Connection": "keep-alive",
                "Content-Type": "application/json",
                "Cookie": "sessionid=pchxc2xo3mg4cyteq3i1wzkdx1cqpgnc",
                "Origin": "https://www.kaceebest.com",
                "Host": "www.kaceebest.com",
                "Referer": str(link_sss),
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": str(random.choice(user_agents)),
            }

            payload = '{"product_id": "' + product_id + '", "val": ["' + value + '"]}'
            session = requests.session()
            req = session.post('https://www.kaceebest.com/dashboard/check_stock', headers=Headers, data=payload)

            print(req.status_code)
            print(req.text)
            print(req.content)

            if req.status_code == 200:
                print("Response received:", req.json())
            else:
                print("Failed to retrieve data. Status code:", req.status_code)
        except Exception as e:
            print('Error:', e)


    except Exception as err:
        print(err)

    # data = {
    #     "ID": ids,
    #     "Name": names,
    #     "Detail": details,
    #     "Image": images
    # }
    #
    # df = pd.DataFrame(data)
    #
    # df.to_excel(r"C:\Users\kc\Desktop\odoo_code\Web_Scraping\files\2024-06-11\kaceebest\report.xlsx", index=False)


def save_link():
    for type in property_type:
        # print(type)
        start_page = property_type[type]['start']
        end_page = property_type[type]['end']+1
        # print(start_page, end_page)
        for i in tqdm(range(start_page, end_page)):
            # print(i)
            url = base_url.replace('placeholder_page', str(i))
            res = requests.get(url)
            # print(res.text)
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, 'html.parser')
            # print(soup)
            link = []
            try:
                links = soup.find_all("div", {"x-data": "ProductCard"})
                for j in links:
                    linkss = j.find("a", class_="product-name")['href']
                    link_url = "https://www.kaceebest.com/" + linkss
                    print(link_url)
                    link.append(link_url)
            except Exception as e:
                print(e)

            file_links = codecs.open(path_links + f"/links_{type}.txt", "a+", "utf-8")
            for l in link:
                file_links.writelines(l + "\n")
            file_links.close()


if __name__ == "__main__":
    loop_link()
    # get_data()
    # save_link()
