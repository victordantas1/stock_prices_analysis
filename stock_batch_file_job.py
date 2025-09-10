import os
import pandas as pd
from polygon import RESTClient

from polygon_data_ingestion import convert_to_points
from polygon_data_ingestion import save_points
from dotenv import load_dotenv

load_dotenv()

def read_parquet_files(directory: str):
    files_list = os.listdir(directory)
    df = pd.DataFrame()
    for file in files_list:
        path = os.path.join('data', file)
        print(f'Reading {path}...')
        df_stock = pd.read_parquet(path)
        df_stock['ticker'] = file.split('_')[0]
        df = pd.concat([df, df_stock])

    return df

if __name__ == '__main__':
    host = os.getenv('INFLUX_HOST')
    token = os.getenv('INFLUX_TOKEN')
    database = os.getenv('INFLUX_DATABASE')
    polygon_api_key = os.getenv("POLYGON_API_KEY")

    client = RESTClient(api_key=polygon_api_key)

    df_stocks = read_parquet_files('data')
    stocks_data = df_stocks.to_dict('records')
    points = convert_to_points(stocks_data)
    save_points(host, token, database, points)