from datetime import timedelta

from mysql.connector.errors import IntegrityError
from sqlalchemy import types

import pandas_expansion
import stock_master
import stock_price
import stock_trend
from loading_from_mysql import *
from mysql_supporter import local_engine
from progress_bar import print_progress_bar


def save_stock_master():
    """
    Get stock masters by stock_master.get_krx_stock_master(),
    save them in MySQL, and return them.

    :return stock_masters: DataFrame
        index       code        | object with 6 length numbers.
        columns     name        | object
                    sector_code | object
                    sector      | object
                    telephone   | object
                    address     | object
    """
    schema_name = 'stock_master'
    print("Scrapping {}...".format(schema_name))
    conn = local_engine.connect()
    try:
        # create table if not exists.
        local_engine.execute(stock_master.create_table_sql)

        existing_stock_masters = get_stock_masters()

        # get all stock codes and insert them.
        stock_masters = stock_master.get_krx_stock_master()

        # get a difference of sets and save it.
        stock_masters = pandas_expansion.difference(stock_masters, existing_stock_masters)
        stock_masters.to_sql(schema_name, conn, if_exists='append', dtype={'code': types.VARCHAR(20)})

    except IntegrityError:
        pass

    except exc.SQLAlchemyError:
        traceback.print_exc()

    finally:
        conn.close()
        print("Scrapping {} of {} is done!!".format(schema_name, len(stock_masters)))

    return stock_masters


def save_stock_trend():
    """
    Get stock trends by stock_code,
    save them in MySQL, and return them.

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
    schema_name = 'stock_trend'
    print("Scrapping {}...".format(schema_name))
    conn = local_engine.connect()
    try:
        # create table if not exists.
        local_engine.execute(stock_trend.create_table_sql)

        # get all stock codes from the database.
        stock_masters = get_stock_masters()

        # get all stock trends.
        progress_bar_size = len(stock_masters)
        progress_bar_count = 1
        print_progress_bar(progress_bar_count, progress_bar_size, prefix='Progress:', suffix='Complete', length=50)
        for code, row in stock_masters.iterrows():
            existing_stock_trends = get_stock_trends(code)
            existing_stock_trends.sort_index(inplace=True)
            if len(existing_stock_trends) is not 0:
                # If we already have old data, we get data after the last date.
                last_date = existing_stock_trends.last_valid_index()
                start_date = last_date + timedelta(days=1)
                stock_trends = stock_trend.get_krx_stock_trend(code, start_date=start_date)
            else:
                # If there is no data, we get all data.
                stock_trends = stock_trend.get_krx_stock_trend(code)

            stock_trends.to_sql(schema_name, conn, if_exists='append', dtype={'code': types.VARCHAR(20)})
            progress_bar_count += 1
            print_progress_bar(progress_bar_count, progress_bar_size, prefix='Progress:', suffix='Complete', length=50)

    except IntegrityError:
        pass

    except exc.SQLAlchemyError:
        traceback.print_exc()

    finally:
        conn.close()
        print("Scrapping {} is done!!".format(schema_name))


def save_stock_price():
    """
    Get stock prices by stock_code,
    save them in MySQL, and return them.

    :return stock_prices: DataFrame
        index       date    | datetime
        columns     open    | int
                    high    | int
                    low     | int
                    close   | int
                    volume  | int
                    code    | object
    """
    schema_name = 'stock_price'
    print("Scrapping {}...".format(schema_name))
    conn = local_engine.connect()
    try:
        # create table if not exists.
        local_engine.execute(stock_price.create_table_sql)

        # get all stock codes from the database.
        stock_masters = get_stock_masters()

        progress_bar_size = len(stock_masters)
        progress_bar_count = 1
        print_progress_bar(progress_bar_count, progress_bar_size, prefix='Progress:', suffix='Complete', length=50)
        for code, row in stock_masters.iterrows():
            existing_stock_prices = get_stock_prices(code)
            existing_stock_prices.sort_index(inplace=True)
            if len(existing_stock_prices) is not 0:
                # If we already have old data, we get data after the last date.
                last_date = existing_stock_prices.last_valid_index()
                start_date = last_date + timedelta(days=1)
                stock_prices = stock_price.get_krx_stock_price(code, start_date=start_date)
            else:
                # If there is no data, we get all data.
                stock_prices = stock_price.get_krx_stock_price(code)

            stock_prices.to_sql(schema_name, conn, if_exists='append', dtype={'code': types.VARCHAR(20)})
            progress_bar_count += 1
            print_progress_bar(progress_bar_count, progress_bar_size, prefix='Progress:', suffix='Complete', length=50)

    except IntegrityError:
        pass

    except exc.SQLAlchemyError:
        traceback.print_exc()

    finally:
        conn.close()
        print("Scrapping {} is done!!".format(schema_name))


if __name__ == "__main__":
    save_stock_master()
    save_stock_trend()
    save_stock_price()
