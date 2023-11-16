import zipfile
import numpy as np
import pandas as pd
import pandasql as ps
import os

from datetime import datetime
from io import BytesIO

import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# Load the first spreadsheet
cleaned_dataset = '../cleaned_dataset.csv'
if not os.path.exists(cleaned_dataset):
    print('Creating cleaned df...')
    print('start ', datetime.now())
    dfs = {}
    zip_files = '../zip_files'

    for zip_file_name in os.listdir(zip_files):
        if zip_file_name.endswith('.zip'):
            zip_file_path = os.path.join(zip_files, zip_file_name)
            # open the zip file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                # iterate through each file in zip folder
                for csv_file_name in zip_file.namelist():
                    # check for csv files
                    if csv_file_name.endswith('.csv'):
                        # read csv from zip folder into df
                        with zip_file.open(csv_file_name) as csv_file:
                            df_name = csv_file_name[:-4]
                            dfs[df_name] = pd.read_csv(BytesIO(csv_file.read()))
    # merge analyst dfs into one
    dfs['analyst_ratings_processed'] = pd.concat([dfs['analyst_ratings_processed_1'], dfs['analyst_ratings_processed_2']], ignore_index=True)
    dfs['analyst_ratings_processed'] = dfs['analyst_ratings_processed'].drop(dfs['analyst_ratings_processed'].columns[0], axis=1)
    dfs['analyst_ratings_processed'] = dfs['analyst_ratings_processed'].dropna()
    # delete smaller dfs as they were merged into one
    del dfs['analyst_ratings_processed_1']
    del dfs['analyst_ratings_processed_2']

    article_titles_with_stocks = dfs['analyst_ratings_processed']
    # Convert 'title' column to string
    article_titles_with_stocks['title'] = article_titles_with_stocks['title'].astype('string')

    # Convert 'date' column to datetime format
    article_titles_with_stocks['date'] = article_titles_with_stocks['date'].apply(lambda x: x.split(' ')[0])
    article_titles_with_stocks['date'] = pd.to_datetime(article_titles_with_stocks['date'])

    # Convert 'stock' column to string
    article_titles_with_stocks['stock'] = article_titles_with_stocks['stock'].astype('string')

    # Load the second spreadsheet
    stock_price_lookup = dfs['sp500_all_assets']
    # stock_price_lookup.index = pd.to_datetime(stock_price_lookup['Date'])

    stock_price_lookup['Date'] = pd.to_datetime(stock_price_lookup['Date'])

    # original data has companies' tickers as each column,
    # convert the data into a dataframe with columns of date, ticker, prices
    df_list = []
    for col in stock_price_lookup.columns:
        if col != 'Date':
            df_temp = stock_price_lookup[['Date', col]]
            df_temp['Ticker'] = col
            df_temp = df_temp.rename(columns={col:"Price"})
            df_temp = df_temp.reset_index(drop=True)
            df_list.append(df_temp)
    stock_price_converted = pd.concat(df_list, axis=0)

    # to calculate daily return, get the price from previous day
    stock_price_converted = stock_price_converted.sort_values(by=['Ticker', 'Date'])
    stock_price_converted['prev_price'] = stock_price_converted.groupby(['Ticker'])['Price'].shift(1)
    # drop rows without price info
    stock_price_converted = stock_price_converted[(stock_price_converted['Price'] != 0) & (stock_price_converted['prev_price'] != 0)]
    stock_return = stock_price_converted.dropna()
    stock_return['return'] = stock_return.apply(lambda x: (x['Price'] - x['prev_price'])/x['prev_price'], axis=1)
    # previous-day return
    stock_return['ret_previous'] = stock_return.groupby(['Ticker'])['return'].shift(1)
    # after-day return
    stock_return['ret_after'] = stock_return.groupby(['Ticker'])['return'].shift(-1)

    # return calculations (p: previous, a: after)
    # ret_p1_a1: average return of the 3-day window t=-1 to t=1 (t=0 is the event day)
    # ret_p3_p1: average return of the 3-day window t=-3 to t=-1
    # ret_a1_a3: average return of the 3-day window t=1 to t=3
    stock_return['ret_p1_a1'] = stock_return.groupby('Ticker')['return'].transform(lambda x: x.rolling(window=3, min_periods=3, center=True).mean())
    stock_return['ret_p3_p1'] = stock_return.groupby(['Ticker'])['ret_p1_a1'].shift(2)
    stock_return['ret_a1_a3'] = stock_return.groupby(['Ticker'])['ret_p1_a1'].shift(-2)
    # ret_p2_a2: average return of the 5-day window t=-2 to t=2 (t=0 is the event day)
    # ret_p5_p1: average return of the 5-day window t=-5 to t=-1
    # ret_a1_a5: average return of the 5-day window t=1 to t=5
    stock_return['ret_p2_a2'] = stock_return.groupby('Ticker')['return'].transform(lambda x: x.rolling(window=5, min_periods=5, center=True).mean())
    stock_return['ret_p5_p1'] = stock_return.groupby(['Ticker'])['ret_p2_a2'].shift(3)
    stock_return['ret_a1_a5'] = stock_return.groupby(['Ticker'])['ret_p2_a2'].shift(-3)

    # drop na
    stock_return = stock_return.dropna()

    query = """
        SELECT *
        FROM article_titles_with_stocks AS news
        LEFT JOIN stock_return AS ret
        ON news.date = ret.Date AND news.stock = ret.Ticker
        ORDER BY Ticker, Date
    """
    merged_df = ps.sqldf(query, locals())
    merged_df = merged_df.drop(columns=['date', 'Ticker'])
    merged_df = merged_df.dropna()
    print(f"Number of observation in the final dataset: {merged_df.shape[0]}")
    print("Writing to csv file in CleanedDataSets directory")
    merged_df.to_csv('cleaned_dataset.csv', header=True, index=False)
    print('end ', datetime.now())
else:
    print("Cleaned df already exists")
    article_titles_with_stocks = pd.read_csv(cleaned_dataset, index_col='date')
# print(article_titles_with_stocks.head())