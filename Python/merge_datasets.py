import pandas as pd

article_titles_with_stocks = pd.read_csv('RawDataSets/analyst_ratings_processed.csv', skiprows=[1])
article_titles_with_stocks = article_titles_with_stocks.dropna()
article_titles_with_stocks = article_titles_with_stocks.drop(article_titles_with_stocks.columns[0], axis=1)

# Convert 'title' column to string
article_titles_with_stocks['title'] = article_titles_with_stocks['title'].astype(str)

# Convert 'date' column to datetime format and then to date in the desired format
article_titles_with_stocks = article_titles_with_stocks.dropna(subset=['date'], how='any')

article_titles_with_stocks['date'] = pd.to_datetime(article_titles_with_stocks['date'])
article_titles_with_stocks['date'] = article_titles_with_stocks.apply(lambda x: x.dt.strftime('%Y-%m-%d'))

# Convert 'stock' column to string
article_titles_with_stocks['stock'] = article_titles_with_stocks['stock'].astype(str)

print(article_titles_with_stocks.dtypes)

stock_price_lookup_raw = pd.read_csv('RawDataSets/SnP500-All-assets.csv', index_col=0)

# Reshape second dataframe
stock_price_lookup_raw = stock_price_lookup_raw.transpose()
stock_price_lookup = stock_price_lookup_raw.melt(id_vars=['stock'], var_name='date', value_name='price-at-close')
stock_price_lookup['date'] = pd.to_datetime(stock_price_lookup['date'], format='%Y-%m-%d').dt.date

# Merge the dataframes based on 'stock' and 'date'
merged_df = pd.merge(article_titles_with_stocks, stock_price_lookup, how='left', left_on=['stock', 'date'],
                     right_on=['stock', 'date'])

# If you want to keep only certain columns in the final dataframe
final_df = merged_df[['title', 'date', 'stock', 'price-at-close']]

final_df.head(20)
