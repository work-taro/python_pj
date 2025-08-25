import os
import dbf
import time
import pandas as pd
import datetime as dt
from threading import Thread
from sqlalchemy import create_engine, text

db_host = 'localhost'
db_database = 'kacee_center'
db_username = 'root'
db_password = ''
db_port = 3306

engine = create_engine("mysql+pymysql://root:@localhost/kacee_center")


def fetchSQL():
    print('======= Fetch ex_customer SQL =======')
    df_customer = pd.read_sql('SELECT cuscod, cusnam FROM ex_customer', engine)
    print(df_customer.head())


fetchSQL()
