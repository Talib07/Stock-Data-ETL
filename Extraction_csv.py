from pyspark.sql import SparkSession
import requests
import os
from dotenv import load_dotenv
from Transformation_csv import transformation
from Loader import load_bigquery

load_dotenv()
api_key = os.getenv("api_key")

# Create a spark session

spark = SparkSession.builder.appName("ET").getOrCreate()

def run_extraction_csv():
    stocks = ['NOK', 'TOSBF', 'NTDOY', 'HTHIY', 'PCRFY']

    for items in stocks:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={items}&apikey={api_key}&datatype=csv'
        response = requests.get(url)
        
        # Send response.content to transformation function also send the stock symbol
        result = transformation(spark,response.content,items)

        # Send the df retuned after transformation to bigquery
        dataset = 'CSVs'
        load_bigquery(result,dataset,items)


run_extraction_csv()