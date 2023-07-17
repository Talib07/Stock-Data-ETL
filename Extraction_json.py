import requests
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from Transformation_json import transformation
from Loader import load_bigquery

# Define a spark session

spark = SparkSession.builder.appName('ET').getOrCreate()

# Loading Alpha Vantage API key
load_dotenv()
api_key = os.getenv("api_key")

def run_extraction_json():
    # Define the stock symbol and the desired output size
    ticker_symbols = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META']

    for symbol in ticker_symbols:
        # Make the API request
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey={api_key}'
        response = requests.get(url)
        data = response.json()
        
        # Pass the json data to the transformation function
        result = transformation(spark,data,symbol)

        # Pass the tramsformed df to google bigquery
        dataset = 'JSONs'
        load_bigquery(result,dataset,symbol)

run_extraction_json()

