import io
from datetime import datetime

import pandas as pd
import requests

import code_converter

columns_map = {'년/월/일': 'date', '종가': 'close', '거래량(주)': 'volume', '기관_매수량(주)': 'institutional_buy',
               '기관_매도량(주)': 'institutional_sell', '기관_순매수(주)': 'institutional_net_buy', '외국인_매수량(주)': 'foreign_buy',
               '외국인_매도량(주)': 'foreign_sell', '외국인_순매수(주)': 'foreign_net_buy'}
usecols = ['년/월/일', '종가', '거래량(주)', '기관_매수량(주)', '기관_매도량(주)',
           '기관_순매수(주)', '외국인_매수량(주)', '외국인_매도량(주)', '외국인_순매수(주)']

create_table_sql = """CREATE TABLE IF NOT EXISTS `stock_trend` (    
        `date` VARCHAR(20),
        `close` DOUBLE,
        `volume` INT,
        `institutional_buy` INT,
        `institutional_sell` INT,
        `institutional_net_buy` INT,
        `foreign_buy` INT,
        `foreign_sell` INT,
        `foreign_net_buy` INT,
        `code` VARCHAR(20),
        PRIMARY KEY (`date`, `code`)
    );
"""


def get_krx_stock_trend(stock_code, start_date=datetime(1900, 1, 1), end_date=datetime(2100, 1, 1)):
    """
    Get korean stock trends from start_date to end_date by scrapping KRX.

    :param stock_code: string with 6 length number.
    :param start_date: datetime
    :param end_date: datetime
    :return stock_trends: DataFrame
        index       date                    | datetime
        columns     close                   | float
                    volume                  | int
                    institutional_buy       | int
                    institutional_sell      | int
                    institutional_net_buy   | int
                    foreign_buy             | int
                    foreign_sell            | int
                    foreign_net_buy         | int
                    code                    | object
    """
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'MKD/10/1002/10020103/mkd10020103_01',
        'isu_cd': code_converter.code_to_isin(stock_code),
        'isu_srt_cd': 'A{}'.format(stock_code),
        'type': 'D',
        'period_strt_dd': start_date.strftime('%Y%m%d'),
        'period_end_dd': end_date.strftime('%Y%m%d'),
        'pagePath': '/contents/MKD/10/1002/10020103/MKD10020103.jsp',
    }

    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.content

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    csv_str = requests.post(down_url, down_data).text
    if csv_str[-1] != '"':  # KRX has a bug. Sometimes, last " is omitted.
        csv_str += '"'

    # Remove the last row, which has sum.
    stock_trends = pd.read_csv(io.StringIO(csv_str), thousands=',', parse_dates=['년/월/일'], skipfooter=1,
                               engine='python')

    # Return empty DataFrame if stock_trends are empty.
    if stock_trends.empty:
        return stock_trends

    stock_trends = stock_trends[usecols]
    # Change column names.
    stock_trends.rename(columns=columns_map, inplace=True)

    # Change the index to date.
    stock_trends.set_index('date', inplace=True)

    # Add a column called 'code'.
    stock_trends['code'] = stock_code

    return stock_trends
