from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, expr, to_timestamp
from pyspark.sql import Window
import csv
from io import StringIO
import codecs
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType, StructType, StructField


# Read the CSV content
def transformation(spark,content,symbol):
    # Decode the bytes content to a string using UTF-8 encoding
    decoded_content = codecs.decode(content, 'utf-8')

    # Create a StringIO object to read the CSV data
    csv_data = StringIO(decoded_content)

    # Read the CSV data using the csv module and convert it to a list of dictionaries
    reader = csv.DictReader(csv_data)
    data = [row for row in reader]

    # Create a DataFrame from the list of dictionaries
    df = spark.createDataFrame(data)

    df = df.withColumnRenamed("timestamp", "date")

    # Add a column stock symbol
    df = df.withColumn('symbol',F.lit(symbol))

    # Specify the window size
    window_size = 4

    # Create a Window specification based on the "date" column
    window_spec = Window.orderBy("date").rowsBetween(Window.currentRow, window_size - 1)

    # Compute the rolling mean
    df = df.withColumn('RollingMean', round(expr(f"avg(Close) OVER (ORDER BY Date ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"), 2))

    # Compute the rolling standard deviation
    df = df.withColumn('RollingStd', round(expr(f"stddev(Close) OVER (ORDER BY Date ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"), 2))

    # Compute the exponential moving average (EMA)
    df = df.withColumn('EMA', round(expr(f"avg(Close) OVER (ORDER BY Date ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"), 2))

    # Compute the weekly returns
    df = df.withColumn('Close_Lag', expr("lag(Close) OVER (ORDER BY Date)"))
    df = df.withColumn('Weekly_Returns', round((col('Close') - col('Close_Lag')) / col('Close_Lag') * 100, 2))
    df = df.drop('Close_Lag')

    # Drop rows with NaN values
    df = df.dropna()

    # Convert the columns to the desired data types
    df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd"))
    df = df.withColumn("open", col("open").cast("float"))
    df = df.withColumn("high", col("high").cast("float"))
    df = df.withColumn("low", col("low").cast("float"))
    df = df.withColumn("close", col("close").cast("float"))
    df = df.withColumn("volume", col("volume").cast("integer"))

    # Return the final df after transformation
    return df