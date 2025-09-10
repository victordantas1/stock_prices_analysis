from typing import List

from polygon.rest import RESTClient
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
import os
from influxdb_client_3 import (
  InfluxDBClient3, InfluxDBError, Point,
  WriteOptions, write_client_options
)
import pandas as pd
from loguru import logger

load_dotenv()

def read_yesterday_data_from_polygon(client: RESTClient):
    one_day = timedelta(days=1)
    yesterday = date.today() - one_day

    logger.info(f'Loading Stocks of {yesterday}')
    daily_grouped_data = client.get_grouped_daily_aggs(
        date=yesterday,
        adjusted="true",
    )
    logger.info('Stocks Loaded')
    return daily_grouped_data

def converting_daily_data_to_dict(daily_grouped_data: List):
    logger.info('Converting stocks to dict...')
    daily_grouped_data_dict = [stock_data.__dict__ for stock_data in daily_grouped_data]
    logger.info('Stocks Converted')
    return daily_grouped_data_dict

def remove_null_values(daily_grouped_data_dict: List):
    df_daily_data = pd.DataFrame(data=daily_grouped_data_dict)
    df_daily_data = df_daily_data.fillna(0)
    daily_grouped_data = df_daily_data.to_dict('records')
    return daily_grouped_data

def convert_to_points(daily_grouped_data: List):
    logger.info('Converting stocks to Points...')
    points = [
        Point('stocks')
        .tag("ticker", stock['ticker'])
        .time(int(stock['timestamp'] * 1000000))
        .field("open", float(stock['open']))
        .field('high', float(stock['high']))
        .field('close', float(stock['close']))
        .field('volume', int(stock['volume']))
        .field('vwap', float(stock['vwap']))
        .field('transactions', int(stock['transactions']))
        for stock in daily_grouped_data
    ]
    logger.info('Stocks Converted to Points')

    return points


def save_points(host, token, database, points: List):
    def success(self, data: str):
        print(f"Successfully wrote batch: data: {data}")

    def error(self, data: str, exception: InfluxDBError):
        print(f"Failed writing batch: config: {self}, data: {data} due: {exception}")

    def retry(self, data: str, exception: InfluxDBError):
        print(f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}")

    # Configure options for batch writing.
    write_options = WriteOptions(
        batch_size=500,
        flush_interval=10_000,
        jitter_interval=2_000,
        retry_interval=5_000,
        max_retries=5,
        max_retry_delay=30_000,
        exponential_base=2,
    )

    wco = write_client_options(
        success_callback=success,
        error_callback=error,
        retry_callback=retry,
        write_options=write_options
    )

    with InfluxDBClient3(
            host=host,
            token=token,
            database=database,
            write_client_options=wco
    ) as client:
        logger.info('Writing Points to InfluxDB...')
        client.write(points)
        logger.info('Points written to InfluxDB')

def main():
    host = os.getenv('INFLUX_HOST')
    token = os.getenv('INFLUX_TOKEN')
    database = os.getenv('INFLUX_DATABASE')
    polygon_api_key = os.getenv("POLYGON_API_KEY")

    client = RESTClient(api_key=polygon_api_key)

    daily_grouped_data = read_yesterday_data_from_polygon(client)
    daily_grouped_data_dict = converting_daily_data_to_dict(daily_grouped_data)
    daily_grouped_data_dict = remove_null_values(daily_grouped_data_dict)

    points = convert_to_points(daily_grouped_data_dict)

    save_points(host, token, database, points)

if __name__ == '__main__':
    main()
