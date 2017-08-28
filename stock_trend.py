import requests
import sqlite3
import pandas as pd
import io
import datetime
import stock_master
import code_converter


# noinspection PyShadowingNames
def get_krx_stock_trend(stock_code, stock_name, start_date, end_date):
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name': 'fileDown',
        'filetype': 'xls',
        'url': 'MKD/10/1002/10020103/mkd10020103_01',
        'isu_cdnm': 'A{}/{}'.format(stock_code, stock_name),
        'isu_cd': code_converter.code_to_isin(stock_code),
        'isu_nm': stock_name,
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

    r = requests.post(down_url, down_data)
    f = io.BytesIO(r.content)
    usecols = ['년/월/일', '종가', '거래량(주)', '기관_순매수(주)', '외국인_순매수(주)']
    df = pd.read_excel(f, converters={'년/월/일': str}, usecols=usecols, thousands=',')

    # 마지막 줄의 '합계' row 삭제
    df = df[:-1]

    # column 명 변경
    df.columns = ['date', 'close', 'volume', 'institutional_net_buying', 'foreign_net_buying']
    df.set_index('date', inplace=True)
    df['code'] = stock_code

    return df

if __name__ == "__main__":
    conn = sqlite3.connect('findata.db')
    sms = stock_master.get_krx_stock_master()

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365)

    stock_trend = pd.DataFrame()
    for index, row in sms.iterrows():
        stock_trend = stock_trend.append(get_krx_stock_trend(row['code'], row['name'], start_date, end_date))
        # 진행 확인용 출력
        print(stock_trend.tail())

    stock_trend.to_sql('stock_trend', conn, if_exists='replace')
    conn.close()
