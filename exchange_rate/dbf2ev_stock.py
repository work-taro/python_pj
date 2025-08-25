import datetime
from dbfread import FieldParser, DBF
import pandas as pd
import os
import time
import numpy as np
import datetime as dt
import pymysql.cursors
from threading import Thread
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, DATE, FLOAT, INTEGER, TIMESTAMP

class MyFieldParser(FieldParser):
    def parseN(self, field, data):
        try:
            value = data.decode(self.encoding, errors='replace').strip()
            try:
                value = float(value)
            except ValueError:
                pass
            return value
        except ValueError:
            return None

    def parseC(self, field, data):
        return data.decode(self.encoding, errors='replace').rstrip('\0 ').replace(u'\xa0', u' ')

    def parseD(self, field, data):
        try:
            return datetime.date(int(data[:4]), int(data[4:6]), int(data[6:8]))
        except ValueError:
            return None


def dbEngine():
    # Center
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(user="kaceecent_er", pw="0gC186!S}Bj6rr", db="kacee_center", host="127.0.0.1"))

    # Dev
    # engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(user="kacee", pw="K_cee2022", db="kacee_center_dev", host="192.168.2.12"))

    return engine

def dbCenter():
    # Center
    conn = pymysql.connect(host='127.0.0.1', port=3306, cursorclass=pymysql.cursors.DictCursor, database='kacee_center', user='kaceecent_er', password='0gC186!S}Bj6rr')

    # Dev
    # conn = pymysql.connect(host='192.168.2.12', port=3306, cursorclass=pymysql.cursors.DictCursor, database='kacee_center_dev', user='kacee', password='K_cee2022')
    return conn

def tbProduct():
    global conn
    conn = None
    try:
        conn = dbCenter()
        if conn.connect:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = ("SELECT ev_product.stkcod as STKCOD, ev_product.stkdes as STKDES, ev_product.qucod as QUCOD, ev_unit.typdes as QUDES, ev_group.typdes as CATEGORY \
                FROM ev_product \
                LEFT JOIN ev_unit ON ev_product.qucod = ev_unit.typcod \
                LEFT JOIN ev_group ON ev_product.stkgrp= ev_group.typcod \
                ORDER BY ev_product.stkcod")
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    except Exception as e:
        print(e)
    finally:
        if conn is not None and conn.connect:
            conn.close()

def module_stloc():
    print('STLOC : Starting...')
    ts = time.time()
    tb_product = tbProduct()
    df_product = pd.DataFrame(tb_product)
    df_product = df_product[['STKCOD', 'STKDES', 'QUCOD', 'QUDES', 'CATEGORY']]

    table = DBF('../DBF_EV/STLOC.DBF', parserclass=MyFieldParser, encoding='cp874', ignore_missing_memofile=True)
    field_names = ['STKCOD', 'LOCCOD', 'LOCBAL', 'UNITPR', 'LOCVAL', 'LMOVDAT']

    stloc_record = []
    for record in table:
        if record["STKCOD"].strip() != '' and record["STATUS"].strip() == 'A':
            stloc_record.append([
                record['STKCOD'].strip(),
                record['LOCCOD'].strip(),
                record['LOCBAL'],
                record['UNITPR'],
                record['LOCVAL'],
                record['LMOVDAT']
            ])

    df_stloc = pd.DataFrame(stloc_record)
    df_stloc = df_stloc.set_axis(field_names, axis=1)
    df = df_stloc.merge(df_product.copy(), on=['STKCOD'])

    stkdes_column = df.pop('STKDES')
    df.insert(1, 'STKDES', stkdes_column)
    df.sort_values(by=['STKCOD', 'STKDES', 'LOCCOD', 'LOCBAL', 'UNITPR', 'LOCVAL', 'LMOVDAT'], inplace=True)
    print('STLOC : Total : ' + str(time.time() - ts) + ' sec.')
    return df

def module_istab():
    print('ISTAB : Starting...')
    ts = time.time()

    table = DBF('../DBF_EV/ISTAB.DBF', parserclass=MyFieldParser, encoding='cp874', ignore_missing_memofile=True)
    field_names = ['TABTYP', 'TABCOD', 'SHORTNAM', 'LOCDES']

    istab_record = []
    for record in table:
        if str(record["TABTYP"]) in ['21']:
            istab_record.append([
                str(record["TABTYP"]),
                str(record["TYPCOD"]),
                str(record["SHORTNAM"]),
                str(record["TYPDES"])
            ])

    df_istab = pd.DataFrame(istab_record)
    df = df_istab.set_axis(field_names, axis=1)
    print('ISTAB : Total : ' + str(time.time() - ts) + ' sec.')
    return df

def process():
    ts = time.time()
    timestamp = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    df_stloc = module_stloc()
    df_istab = module_istab()
    print('ev_stocks : Starting...')
    # Merge data
    df = df_stloc.merge(df_istab, left_on='LOCCOD', right_on='TABCOD', how='left')

    # Replace all NaN values with empty string
    df.fillna('', inplace=True)

    # Query database
    # Formatting floating point columns
    df['LOCBAL'] = df['LOCBAL'].astype(float).round(2)
    df['UNITPR'] = df['UNITPR'].astype(float).round(2)
    df['LOCVAL'] = df['LOCVAL'].astype(float).round(2)

    # Handle LMOVDAT parsing
    def parse_date(x):
        if isinstance(x, dt.date):
            return x
        elif isinstance(x, str) and x:
            try:
                return dt.datetime.strptime(x, '%Y-%m-%d').date()
            except ValueError:
                return None
        else:
            return None

    df['LMOVDAT'] = df['LMOVDAT'].apply(parse_date)
    df['CREATED_AT'] = timestamp
    df['UPDATED_AT'] = timestamp
    df['CREATED_AT'] = pd.to_datetime(df['CREATED_AT'])
    df['UPDATED_AT'] = pd.to_datetime(df['UPDATED_AT'])

    df = df.loc[:, ['STKCOD', 'STKDES', 'LOCCOD', 'LOCDES', 'LOCBAL', 'QUCOD', 'QUDES', 'UNITPR', 'LOCVAL', 'CATEGORY', 'LMOVDAT', 'CREATED_AT', 'UPDATED_AT']]

    # Export
    # df.to_excel(r'..\exports\STLOC.xlsx', sheet_name='Sheet1', index=False)

    # Insert MySQL
    df.reset_index()
    df = df.set_axis([
        'sku',
        'name',
        'storage',
        'storage_des',
        'qty',
        'unit',
        'unit_des',
        'average_cost',
        'residual_value',
        'category',
        'lmov_date',
        'created_at',
        'updated_at'
    ], axis=1)
    df.index += 1  # <- increment default index by 1
    dtypedict = {
        'sku': VARCHAR(100),
        'name': VARCHAR(255),
        'storage': VARCHAR(15),
        'storage_des': VARCHAR(255),
        'qty': FLOAT(),
        'unit': VARCHAR(5),
        'unit_des': VARCHAR(45),
        'average_cost': FLOAT(),
        'residual_value': FLOAT(),
        'sale_category': VARCHAR(255),
        'lmov_date': DATE(),
        'created_at': TIMESTAMP,
        'updated_at': TIMESTAMP
    }

    try:
        engine = dbEngine()
        table_name = 'ev_stocks'
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {table_name};'))
            df.to_sql(table_name, con=engine, if_exists='append', index_label='id', dtype=dtypedict, chunksize=1000)
            conn.execute(text(f'ALTER TABLE {table_name} MODIFY COLUMN id INT AUTO_INCREMENT PRIMARY KEY;'))
            conn.execute(text(f'ALTER TABLE {table_name} ENGINE=InnoDB;'))

        print(f'{table_name} : Completed in ' + str(time.time() - ts) + ' seconds.')
    except Exception as e:
        print("Error Create ev_stock in: ", e)
    finally:
        if engine is not None and engine.connect:
            conn.close()

def quit_program():
    time.sleep(2)
    os._exit(0)

if __name__ == "__main__":
    print('=============== START PROGRAME =================')
    thread = Thread(target=process)
    thread.start()
    thread.join()
    print('================ END PROGRAME ==================')
    quit_program()
