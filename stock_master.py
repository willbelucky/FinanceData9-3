import requests
import sqlite3
import pandas as pd
import io

create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_master (
        code VARCHAR(20) NOT NULL PRIMARY KEY,
        name VARCHAR(50),
        sector_code VARCHAR(30),
        sector VARCHAR(80)
    )
"""


def get_krx_stock_master():
    """
    Get current korean stock masters by scrapping KRX.

    :return stock_masters: DataFrame
        index       code        | object with 6 length numbers.
        columns     name        | object
                    sector_code | object
                    sector      | object
                    telephone   | object
                    address     | object
    """
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name': 'fileDown',
        'filetype': 'xls',
        'url': 'MKD/04/0406/04060100/mkd04060100_01',
        'market_gubun': 'ALL',  # ''ALL':전체, STK': 코스피
        'isu_cdnm': '전체',
        'sort_type': 'A',
        'std_ind_cd': '01',
        'lst_stk_vl': '1',
        'in_lst_stk_vl': '',
        'in_lst_stk_vl2': '',
        'pagePath': '/contents/MKD/04/0406/04060100/MKD04060100.jsp',
    }

    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.content

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    r = requests.post(down_url, down_data)
    f = io.BytesIO(r.content)

    usecols = ['종목코드', '기업명', '업종코드', '업종']
    stock_masters = pd.read_excel(f, converters={'종목코드': str, '업종코드': str}, usecols=usecols)
    stock_masters.columns = ['code', 'name', 'sector_code', 'sector']

    # Change the index to code.
    stock_masters.set_index('code', inplace=True)

    return stock_masters
