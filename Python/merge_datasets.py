import zipfile
from io import TextIOWrapper
import os
import tempfile
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, lag, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType

from datetime import datetime

import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# Create a Spark session
spark = SparkSession.builder.appName("StockAnalysis").getOrCreate()

# Load the first spreadsheet
cleaned_dataset = '../updated_cleaned_dataset.csv'

if not os.path.exists(cleaned_dataset):
    print('Creating cleaned df...')
    print('start ', datetime.now())
    dfs = {}
    zip_files = '../zip_files'

    # Schema for article_titles_with_stocks DataFrame
    article_titles_with_stocks_schema = StructType([
        StructField("title", StringType(), True),
        StructField("date", DateType(), True),
        StructField("stock", StringType(), True)
    ])

    # Create an empty DataFrame with the specified schema
    article_titles_with_stocks = spark.createDataFrame([], schema=article_titles_with_stocks_schema)

    for zip_file_name in os.listdir(zip_files):
        if zip_file_name.endswith('.zip'):
            zip_file_path = os.path.join(zip_files, zip_file_name)

            # Read CSV files directly into Spark DataFrame
            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                for csv_file_name in zip_file.namelist():
                    if csv_file_name.endswith('.csv'):
                        df_name = csv_file_name[:-4]

                        # Read CSV content from BytesIO
                        with zip_file.open(csv_file_name) as csv_file:
                            text_io_wrapper = TextIOWrapper(csv_file, encoding='utf-8')
                            csv_content = text_io_wrapper.read()

                        # Save the CSV content to a temporary file
                        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_csv:
                            temp_csv.write(csv_content)
                            temp_csv_path = temp_csv.name

                        # Create DataFrame from the temporary file
                        df = spark.read.csv(temp_csv_path, header=True, inferSchema=True)

                        # Clean up the temporary file
                        os.remove(temp_csv_path)

                        dfs[df_name] = df

    # Merge analyst dfs into one
    analyst_ratings_processed = dfs['analyst_ratings_processed_1'].union(dfs['analyst_ratings_processed_2'])
    analyst_ratings_processed = analyst_ratings_processed.drop(dfs['analyst_ratings_processed_1'].columns[0])

    # Convert columns to appropriate types
    article_titles_with_stocks = article_titles_with_stocks.withColumn("title", col("title").cast("string"))
    article_titles_with_stocks = article_titles_with_stocks.withColumn("date", expr("CAST(SUBSTRING_INDEX(date, ' ', 1) AS DATE)"))
    article_titles_with_stocks = article_titles_with_stocks.withColumn("stock", col("stock").cast("string"))

    # Load the second spreadsheet
    stock_price_lookup = dfs['sp500_all_assets']

    # Convert the data into a dataframe with columns of date, ticker, prices
    stock_price_converted = stock_price_lookup.select("Date",
                                                      *[col(col_name).cast("double").alias("Price") for col_name in
                                                        stock_price_lookup.columns if col_name != "Date"])

    # Add a new column for Ticker
    for col_name in stock_price_lookup.columns:
        if col_name != "Date":
            stock_price_converted = stock_price_converted.withColumn("Ticker", lit(col_name))

    # Calculate daily return
    windowSpec = Window().partitionBy("Ticker").orderBy("Date")
    stock_price_converted = stock_price_converted.withColumn("prev_price", lag("Price").over(windowSpec))
    stock_price_converted = stock_price_converted.filter((col("Price") != 0) & (col("prev_price") != 0))
    stock_return = stock_price_converted.dropna().withColumn("return", (col("Price") - col("prev_price")) / col("prev_price"))

    # Calculate previous-day and after-day returns
    stock_return = stock_return.withColumn("ret_previous", lag("return").over(windowSpec))
    stock_return = stock_return.withColumn("ret_after", lag("return", -1).over(windowSpec))

    # Return calculations
    stock_return = stock_return.drop("Price", "prev_price")
    stock_return = stock_return.withColumn("ret_p1_a1", expr("AVG(return) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"))
    stock_return = stock_return.withColumn("ret_p3_p1", lag("ret_p1_a1", 2).over(windowSpec))
    stock_return = stock_return.withColumn("ret_a1_a3", lag("ret_p1_a1", -2).over(windowSpec))
    stock_return = stock_return.withColumn("ret_p2_a2", expr("AVG(return) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)"))
    stock_return = stock_return.withColumn("ret_p5_p1", lag("ret_p2_a2", 3).over(windowSpec))
    stock_return = stock_return.withColumn("ret_a1_a5", lag("ret_p2_a2", -3).over(windowSpec))

    # Drop unnecessary columns
    stock_return = stock_return.drop("Price", "prev_price")

    # SQL Join to merge DataFrames
    merged_df = article_titles_with_stocks.join(
        stock_return,
        (col("date") == col("Date")) & (col("stock") == col("Ticker")),
        "left_outer"
    ).orderBy("Ticker", "Date")

    merged_df = merged_df.drop("date", "Ticker").na.drop()

    print(f"Number of observations in the final dataset: {merged_df.count()}")
    print("Writing to csv file in CleanedDataSets directory")
    merged_df.write.csv('updated_cleaned_dataset.csv', header=True, mode='overwrite')
    print('end ', datetime.now())

else:
    print("Cleaned df already exists")
    article_titles_with_stocks = spark.read.csv(cleaned_dataset, header=True)

# Show the first few rows of the resulting DataFrame
article_titles_with_stocks.show()
