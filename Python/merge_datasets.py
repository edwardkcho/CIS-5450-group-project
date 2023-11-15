import zipfile
import numpy as np
import pandas as pd
import os

from datetime import datetime
from io import BytesIO

# Load the first spreadsheet
cleaned_dataset = 'cleaned_dataset.csv'
if not os.path.exists(cleaned_dataset):
    print('Creating cleaned df...')
    print('start ', datetime.now())
    dfs = {}
    zip_files = 'zip_files'

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
    stock_price_lookup.index = pd.to_datetime(stock_price_lookup['Date'])

    # Add new column to get stock price info in articles df
    article_titles_with_stocks['closing-price'] = np.NAN

    for index, row in article_titles_with_stocks.iterrows():
        date_lookup = str(article_titles_with_stocks.at[index, 'date']).split(' ')[0]
        stock_lookup = article_titles_with_stocks.at[index, 'stock']
        try:
            article_titles_with_stocks.at[index, 'closing-price'] = \
                stock_price_lookup.loc[date_lookup, stock_lookup].round(2)
        except KeyError:
            # already created column as np.NaN, dont need to reassign if no data
            pass

    article_titles_with_stocks = article_titles_with_stocks.dropna()
    print("Writing to csv file in CleanedDataSets directory")
    article_titles_with_stocks.to_csv('cleaned_dataset.csv', header=True, index=False)
    print('end ', datetime.now())
else:
    print("Cleaned df already exists")
    article_titles_with_stocks = pd.read_csv(cleaned_dataset, index_col='date')
# print(article_titles_with_stocks.head())