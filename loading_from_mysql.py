import traceback

import pandas as pd
from sqlalchemy import exc

from mysql_supporter import local_engine


def get_stock_masters(sector_code=None):
    """
    Get stock masters from MySQL and return them.

    :return stock_masters: DataFrame
        index       code        | object with 6 length numbers.
        columns     name        | object
                    sector_code | object
                    sector      | object
                    telephone   | object
                    address     | object
    """
    conn = local_engine.connect()
    schema_name = 'stock_master'
    select_sql = "SELECT * FROM {}".format(schema_name)

    if sector_code is not None:
        select_sql += " WHERE sector_code = {}".format(sector_code)

    try:
        # get all stock codes from the database.
        stock_masters = pd.read_sql(select_sql, conn)
        stock_masters.set_index('code', inplace=True)

    except exc.SQLAlchemyError:
        traceback.print_exc()

    finally:
        conn.close()

    return stock_masters


def get_stock_prices(code=None):
    """
    Get all stock prices from MySQL and return them.

    :return stock_prices: DataFrame
        index       date    | datetime
        columns     open    | int
                    high    | int
                    low     | int
                    close   | int
                    volume  | int
                    code    | object
    """
    conn = local_engine.connect()
    schema_name = 'stock_price'
    select_sql = "SELECT * FROM {}".format(schema_name)

    if code is not None:
        select_sql += " WHERE code = {}".format(code)

    try:
        # get all stock codes from the database.
        stock_prices = pd.read_sql(select_sql, conn, parse_dates=['date'])

    except exc.SQLAlchemyError:
        traceback.print_exc()

    finally:
        conn.close()

    return stock_prices


def get_stock_trends(code=None):
    """
    Get all stock trends from MySQL and return them.

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
    conn = local_engine.connect()
    schema_name = 'stock_trend'
    select_sql = "SELECT * FROM {}".format(schema_name)

    if code is not None:
        select_sql += " WHERE code = {}".format(code)

    try:
        # get all stock codes from the database.
        stock_trends = pd.read_sql(select_sql, conn, parse_dates=['date'])

    except exc.SQLAlchemyError:
        traceback.print_exc()

    finally:
        conn.close()

    return stock_trends


if __name__ == "__main__":
    print(get_stock_masters().tail())
    print(get_stock_prices().tail())
    print(get_stock_trends().tail())
