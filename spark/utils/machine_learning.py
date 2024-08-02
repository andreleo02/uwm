from datetime import datetime, timedelta
import json
import logging
import pytz
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, split, to_timestamp, udf
from pyspark.sql.types import DoubleType, StringType

def apply_longlag_transformations(logger, df):
    """Convert the column lat_long containing a json to two columns."""
    logger.info("Starting apply_longlag_transformations")
    
    df = df.withColumn("latitude", split(col("lat_long"), ", ")[0].cast(DoubleType()))
    df = df.withColumn("longitude", split(col("lat_long"), ", ")[1].cast(DoubleType()))
    df = df.dropna(subset=["latitude", "longitude"])
    
    logger.info(f"DEBG LATLONG COUNT {df.count()} entries")
    return df

def extract_date(json_str):
    try:
        return json.loads(json_str).get("$date", json_str)
    except Exception as e:
        return json_str

extract_date_udf = udf(extract_date, StringType())

def apply_datetime_transformations(logger, df):
    """Parse date column."""
    logger.info("Starting apply_datetime_transformations")
    df = df.dropna("all", subset=["time"])
    
    if 'time' in df.columns:
        time_column_type = df.schema['time'].dataType.simpleString()
        logger.info(f"Time column type: {time_column_type}")

        if time_column_type == 'struct<$date:string>':
            df = df.withColumn("time", to_timestamp(col("time.$date"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
        else:
            df = df.withColumn("time_extracted", extract_date_udf(col("time")))
            df = df.withColumn("time", to_timestamp(col("time_extracted"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')).drop("time_extracted")

    df = df.dropna("all", subset=["time"])
    logger.info(f"Schema after datetime transformations: {df.schema}")
    logger.info(f"Row count after transformations: {df.count()}")
    return df

def main_cleaning(logger, df_bins, df_weather):
    logger.info("Main cleaning")
    df_bins = apply_datetime_transformations(logger, df_bins)
    df_bins = apply_longlag_transformations(logger, df_bins)
    df_weather = apply_datetime_transformations(logger, df_weather)
    df_weather = apply_longlag_transformations(logger, df_weather)

    # Additional cleaning specific to df_weather
    df_weather = df_weather.dropDuplicates(["time"]).drop("battery", "dev_id", "sensor_name", "date", "time_only", "latitude", "longitude", "_id", "lat_long")
    df_bins = df_bins.withColumnRenamed("temperature", "bin_temperature")

    logger.info("Completed cleaning")
    logger.info(f"Final bins schema: {df_bins.schema}")
    logger.info(f"Final weather schema: {df_weather.schema}")
    return df_bins, df_weather

def main_merging(logger, df_bins, df_weather):
    utc_now = datetime.now(pytz.utc)
    melbourne_now = utc_now.astimezone(pytz.timezone('Australia/Melbourne'))
    threshold_time_utc = (melbourne_now - timedelta(hours=300)).astimezone(pytz.utc)
    df_weather = df_weather.filter(F.col('time') >= threshold_time_utc)
    df_bins = df_bins.filter(F.col('time') >= threshold_time_utc)

    grouped_bins = df_bins.groupBy("dev_id").agg({"fill_level": "last", "time": "last"})
    last_row = df_weather.select('time', 'precipitation', 'strikes', 'windspeed', 'airtemp').orderBy(F.desc("time")).limit(1)

    # if last_row['precipitation'] < 1:
    #     print("NO RAIN")

    print(last_row.show())
    print(grouped_bins.show())
    logger.info(f"Last weather data: {last_row.show()}")
    logger.info(f"Grouped bins data: {grouped_bins.show()}")
    return

def main_ml(logger, spark, df_bins, df_weather):
    logger.info("Starting machine learning preparations")
    bins_clean, weather_clean = main_cleaning(logger, df_bins, df_weather)
    bins_clean = bins_clean.filter((col("fill_level") >= 0) & (col("fill_level") <= 100))
    main_merging(logger, bins_clean, weather_clean)
    logger.info("Merging finished")
    return



########### Execuzione di prova  da locale #################

# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#     .appName("Read JSON in Spark") \
#     .getOrCreate()
# df_bins = spark.read.json("spark/utils/bins.json")
# df_weather = spark.read.json("spark/utils/weather.json")

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# main_ml(logger,spark, df_bins, df_weather)