# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2017. 9. 22.
"""
from mysql_supporter import local_engine
import pandas as pd
from loading_from_mysql import get_stock_masters


def save_csv(codes):
    print("start save_csv...")
    sqls = []
    for code in codes:
        sqls.append("""
SELECT stock_price.`date`, stock_price.`close`, stock_trend.`institutional_net_buy`, stock_trend.`foreign_net_buy`
FROM stock_price
INNER JOIN stock_trend ON stock_price.`date`=stock_trend.`date` AND stock_price.`code`=stock_trend.`code`
WHERE stock_price.`code` = {} AND stock_price.`date` <= '2017-08-31 00:00:00' AND stock_price.`date` >= '2016-09-01 00:00:00' ORDER BY stock_price.`date` ASC
""".format(code))

    stock_master = get_stock_masters()
    names = []
    for code in codes:
        name = stock_master.loc[code]['name']
        names.append(name)

    for sql, code, name in zip(sqls, codes, names):
        conn = local_engine.connect()

        df = pd.read_sql(sql, conn, parse_dates=['date'])
        df.to_csv(code+'_'+name+'.csv')

    print("save_csv is done!!")
    return


if __name__ == "__main__":
    codes = ['034950', '025770', '069510', '066410', '033790', '048260', '042110', '033200', '145020', '048830']
    save_csv(codes)
