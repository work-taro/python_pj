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
import urllib.request
import re


# date = datetime.now().strftime('%Y-%m-%d')
date = '2024-06-11'
base_url = "https://www.kaceebest.com/fn/post_cat/home-ideas"
# base_url = "https://www.kaceebest.com/fn/product_search/?page=1"
web = "kaceebest"

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

ids = []
name_of_images = []
link_images = []
descriptions = []


def get_data():
    id = 0
    url = base_url
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    # print(soup)
    try:
        ids.append(id)
        try:
            name_of_pic = soup.find_all("div", class_="card col-md-3 border-0")
            for i in name_of_pic:
                name = i.find("img", class_="card-img-top")['src']
                if "https" not in name:
                    name = "https://www.kaceebest.com" + name
                # name = i.find("img", {"alt": "..."})
                # print(name)
                id += 1
                name_of_images.append(name)
            print("name of img", name_of_images)
            print("ID count :  ", id)
            # print(name_of_pic)
        except Exception as e:
            print('name : ', e)
            name_of_images.append(None)
        try:
            des = soup.find_all('a', class_="card-title fw-bold")
            # print(des)
            for d in des:
                desc = d.find('p')
                if desc is not None:
                    # print("Y", desc.text)
                    descriptions.append(desc.text.strip())
                else:
                    print("No <p> tag found in this <a> tag")
            print("Descriptions : ", descriptions)
        except Exception as e:
            print('detail : ', e)
            descriptions.append(None)
        data = {
            "image_link": name_of_images,
            "description": descriptions
        }
        df = pd.DataFrame(data)
        df.to_excel(r"C:\Users\kc\Desktop\odoo_code\Web_Scraping\images\report.xlsx", index=False)

    except Exception as err:
        print(err)


def get_img():
    source = 'files/report.xlsx'
    data = pd.read_excel(source)
    # print(data)

    for description, image_link in zip(data['description'], data['image_link']):
        des = re.sub(r'[^\w\-_\. ]', '_', description)
        urllib.request.urlretrieve(image_link, "images/" + des + ".jpg")

        # image = datas['description']
        # print(image)
        # urllib.request.urlretrieve(data, "images/" + image)

    # for img in data['house_pictures']:
    #     sleep(0.25)
    #     i += 1
    #     # print(img)
    #     try:
    #         if str(img) != "NULL":
    #             dir_split = str(img).replace("https://propertyforsale.kasikornbank.com/Images/", "").split("/")
    #             _dir = ""
    #             _filename = ""
    #             for j in range(len(dir_split)):
    #                 if j != len(dir_split) - 1:
    #                     _dir += "/" + str(dir_split[j])
    #                     if not os.path.isdir(dirName + _dir):
    #                         os.mkdir(dirName + _dir)
    #                 else:
    #                     _filename = str(dir_split[j])
    #             urllib.request.urlretrieve(img, dirName + _dir + "/" + _filename)
    #         print("Process", i, ":", img)
    #     except Exception as err:
    #         print(err, str(img))

    # print(data)


if __name__ == "__main__":
    # get_data()
    get_img()
