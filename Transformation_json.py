from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, expr, to_timestamp


def transformation(spark,data,symbol):
  json_example = {"WeeklyTimeSeries": data["Weekly Time Series"]}

  df = spark.read.json(spark.sparkContext.parallelize([json_example]))
  result = df.select(F.array(*[
      F.struct(F.lit(i).alias("date"),
              F.col("WeeklyTimeSeries")[i]['1. open'].alias("open"),
              F.col("WeeklyTimeSeries")[i]['2. high'].alias("high"),
              F.col("WeeklyTimeSeries")[i]['3. low'].alias("low"),
              F.col("WeeklyTimeSeries")[i]['4. close'].alias("close"),
              F.col("WeeklyTimeSeries")[i]['5. volume'].alias("volume"))
      for i in df.select("WeeklyTimeSeries.*").columns
  ]).alias("WeeklyTimeSeries")).selectExpr("inline(WeeklyTimeSeries)")

  # Add a column stock symbol
  result = result.withColumn('symbol',F.lit(symbol))

  # Specify the window size
  window_size = 4

  # Compute the rolling mean
  result = result.withColumn('RollingMean', round(expr(f"avg(Close) OVER (ORDER BY Date ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"), 2))

  # Compute the rolling standard deviation
  result = result.withColumn('RollingStd', round(expr(f"stddev(Close) OVER (ORDER BY Date ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"), 2))

  # Compute the exponential moving average (EMA)
  result = result.withColumn('EMA', round(expr(f"avg(Close) OVER (ORDER BY Date ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"), 2))

  # Compute the weekly returns
  result = result.withColumn('Close_Lag', expr("lag(Close) OVER (ORDER BY Date)"))
  result = result.withColumn('Weekly_Returns', round((col('Close') - col('Close_Lag')) / col('Close_Lag') * 100, 2))
  result = result.drop('Close_Lag')

  # Drop rows with NaN values
  result = result.dropna()

  # Change the datatype of the existing column
  result = result.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd"))
  result = result.withColumn("open", col("open").cast("float"))
  result = result.withColumn("high", col("high").cast("float"))
  result = result.withColumn("low", col("low").cast("float"))
  result = result.withColumn("close", col("close").cast("float"))
  result = result.withColumn("volume", col("volume").cast("int"))

  # Return the transformed dataframe
  return result

