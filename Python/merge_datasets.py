import numpy as np
import pandas as pd
import datetime as dt

# Load the first spreadsheet
article_titles_with_stocks = pd.read_csv('../RawDataSets/analyst_ratings_processed.csv', skiprows=[1])
article_titles_with_stocks = article_titles_with_stocks.dropna()
article_titles_with_stocks = article_titles_with_stocks.drop(article_titles_with_stocks.columns[0], axis=1)

# Convert 'title' column to string
article_titles_with_stocks['title'] = article_titles_with_stocks['title'].astype('string')

# Convert 'date' column to datetime format
article_titles_with_stocks['date'] = article_titles_with_stocks['date'].apply(lambda x: x.split(' ')[0])
article_titles_with_stocks['date'] = pd.to_datetime(article_titles_with_stocks['date'])

# Convert 'stock' column to string
article_titles_with_stocks['stock'] = article_titles_with_stocks['stock'].astype('string')

# Load the second spreadsheet
stock_price_lookup = pd.read_csv('../RawDataSets/SnP500-All-assets.csv', index_col=0, skiprows=[0, 2])

stock_price_lookup.index = pd.to_datetime(stock_price_lookup.index)

stock_price_lookup = stock_price_lookup[[x for x in stock_price_lookup.columns if '.' not in x]]

stock_price_lookup = stock_price_lookup.dropna(axis=1, how='all')

# print(stock_price_lookup.head(5))
# print(str(stock_price_lookup.loc['2010-01-04', 'A']))

# Add new column to get stock price info in articles df
article_titles_with_stocks['closing-price'] = np.NAN

for index, row in article_titles_with_stocks.iterrows():
    # row['closing-price'] = stock_price_lookup[row.at[1, 'date'], row.at[1, 'stock']]
    date_lookup = str(article_titles_with_stocks.at[index, 'date']).split(' ')[0]
    stock_lookup = article_titles_with_stocks.at[index, 'stock']
    test = str(stock_price_lookup.loc['2020-06-03', 'A'])
    try:
        article_titles_with_stocks.at[index, 'closing-price'] = \
            stock_price_lookup.loc[date_lookup, stock_lookup]
    except KeyError:
        article_titles_with_stocks.at[index, 'closing-price'] = np.nan

# Merge the dataframes based on 'stock' and 'date'
article_titles_with_stocks.dropna()
# Display the resulting DataFrame
print(article_titles_with_stocks.head(5))
print("Writing to csv file in CleanedDataSets directory")

article_titles_with_stocks.to_csv('../CleanedDataSets/cleaned_dataset.csv', header=True, index=False)