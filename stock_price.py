import io
import sqlite3
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
import requests

import code_converter
import stock_master

columns_map = {'년/월/일': 'date', '종가': 'close', '대비': 'change', '거래량(주)': 'volume', '거래대금(원)': 'trading_value',
               '시가': 'open', '고가': 'high', '저가': 'low', '시가총액(백만)': 'mar_cap', '상장주식수(주)': 'stock_count'}

create_table_sql = """CREATE TABLE if not exists `stock_price` (    
        `date` DATETIME,
        `open` INT,
        `high` INT,
        `low` INT,
        `close` INT,
        `volume` INT,
        `code` VARCHAR(20),
        PRIMARY KEY (`date`, `code`)
    );
"""


def get_krx_stock_price(stock_code, start_date=datetime(1900, 1, 1), end_date=datetime(2100, 1, 1)):
    """
    Getting korean stock prices from start to end by scrapping KRX.

    :param stock_code: string with 6 length number.
    :param start_date: datetime
    :param end_date: datetime

    :return stock_prices: DataFrame
        index       date    | datetime
        columns     open    | int
                    high    | int
                    low     | int
                    close   | int
                    volume  | int
                    code    | object
    """
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    # STEP 01: Generate OTP
    gen_otp_url = "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
    gen_otp_data = {
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'MKD/04/0402/04020100/mkd04020100t3_02',
        'isu_cd': code_converter.code_to_isin(stock_code),
        'fromdate': start_str,
        'todate': end_str,
    }

    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.text

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    csv_str = requests.post(down_url, down_data).text
    if csv_str[-1] != '"':  # KRX has a bug. Sometimes, last " is omitted.
        csv_str += '"'
    stock_prices = pd.read_csv(io.StringIO(csv_str), thousands=',', parse_dates=['년/월/일'])

    # Return empty DataFrame if stock_prices are empty.
    if stock_prices.empty:
        return stock_prices

    stock_prices.rename(columns=columns_map, inplace=True)

    stock_prices = stock_prices[['date', 'open', 'high', 'low', 'close', 'volume']]

    # Add a column called 'code'.
    stock_prices['code'] = stock_code

    # Change the index for date.
    stock_prices.set_index('date', inplace=True)

    return stock_prices

