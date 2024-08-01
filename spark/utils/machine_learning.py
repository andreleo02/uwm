from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, date_format, when, split, col, concat_ws
from pyspark.sql.types import DoubleType,StringType
import ast
import logging, time, json
from pyspark.sql.functions import col, udf, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, DoubleType
import ast
import logging

def apply_longlag_transformations(logger, df):
    """
    
    COnvert the column lat_long containing a json to two columns
    
    """
    logger.info("Starting apply_longlag_transformations")
    
    df = df.withColumn("latitude", split(col("lat_long"), ", ")[0])
    df = df.withColumn("longitude", split(col("lat_long"), ", ")[1])

    # Cast to double if required
    df = df.withColumn("latitude", col("latitude").cast("double"))
    df = df.withColumn("longitude", col("longitude").cast("double"))

    df = df.dropna(subset=["latitude", "longitude"])
    logger.info(f"DEBG LATLONG COUNT {df.count()} entries")

    return df

def extract_date(json_str):
    try:
        return json.loads(json_str).get("$date", json_str)
    except:
        return json_str

extract_date_udf = udf(extract_date, StringType())

def apply_datetime_transformations(logger, df):
    """
    PARSE DATE COLUMN
    """
    logger.info("Starting apply_datetime_transformations")
    df = df.dropna("all", subset=["time"])
    
    if 'time' in df.columns:
        time_column_type = df.schema['time'].dataType.simpleString()
        logger.info(f"Time column type: {time_column_type}")
        print(f"Time column type: {time_column_type}")

        if time_column_type == 'struct<$date:string>':
            # Handle time as struct with $date field
            df = df.withColumn("time", to_timestamp(col("time.$date"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
        else:
            # Handle mixed formats
            df = df.withColumn("time_extracted", extract_date_udf(col("time")))
            df = df.withColumn("time", to_timestamp(col("time_extracted"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
            df = df.drop("time_extracted")

    print("apply datetime", df.count())
    df = df.dropna("all", subset=["time"])

    logger.info(f"Schema after datetime transformations: {df.schema}")
    logger.info(f"Row count after transformations: {df.count()}")
    return df

def main_cleaning(logger, df_bins, df_weather):
    logger.info("Main cleaning")

    df_bins = apply_datetime_transformations(logger, df_bins)
    df_bins = apply_longlag_transformations(logger, df_bins)
    logger.info(f"df bins LATLONG COUNT {df_bins.count()} entries")

    df_weather = apply_datetime_transformations(logger, df_weather)
    df_weather = apply_longlag_transformations(logger, df_weather)
    
    # Additional cleaning specific to df_weather
    df_weather = df_weather.dropDuplicates(["time"]).drop("battery", "dev_id", "sensor_name","date", "time_only","latitude", "longitude","_id","lat_long")
    df_bins = df_bins.withColumnRenamed("temperature", "bin_temperature")

    logger.info("Completed cleaning")
    logger.info(f"Final bins schema: {df_bins.schema}")
    logger.info(f"Final weather schema: {df_weather.schema}")

    return df_bins, df_weather


def main_merging(logger, df_bins, df_weather):
    """
    Keep only data from last 48 hours (to modify)
    Group the bins by device id and keep the last entry
    Keep the last rilevation of the weather
    Keep the last rilevation of pedestrian data (to add)

    Prepare the data for the predictions
    """


    from pyspark.sql import functions as F
    from pyspark.sql.functions import desc

    from datetime import datetime, timedelta
    import pytz

    # keep only 36 hours data
    utc_now = datetime.now(pytz.utc)
    melbourne_now = utc_now.astimezone(pytz.timezone('Australia/Melbourne'))
    threshold_time = melbourne_now - timedelta(hours=300)
    threshold_time_utc = threshold_time.astimezone(pytz.utc)
    df_weather = df_weather.filter(F.col('time') >= threshold_time_utc)
    df_bins = df_bins.filter(F.col('time') >= threshold_time_utc)


    # Group the bins by device id and keep the last result    
    grouped_bins = df_bins.groupBy("dev_id").agg({"fill_level": "last",
                                                 "time": "last",
                                                 })
    
    # Weather data. Select the columns first, then order by "time" and get the first row
    selected_columns = df_weather.select('time', 'precipitation', 'strikes', 'windspeed', 'airtemp')
    last_row = selected_columns.orderBy(desc("time")).limit(1)
    print("LAST ROW", last_row.show())
    print("grouped_bins", grouped_bins.show())

    # Pedestrian data. Select the columns first, then order by "time" and get the first row


    # Predictions
    if last_row['precipitation'] < 1:
        print("NO RAIN")


    return 


def main_ml(logger, spark, df_bins, df_weather):
    from pyspark.sql.functions import col

    bins_clean, weather_clean = main_cleaning(logger, df_bins, df_weather)
    logger.info("Cleaning finished!!")
    bins_clean = bins_clean.filter((col("fill_level") >= 0) & (col("fill_level") <= 100))

    main_merging(logger, bins_clean, weather_clean)

    logger.info("Merging finished")
    #logger.info("Number of rows after cleaning: %d", df_clean.count())

    return 


########### Execuzione #################

# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#     .appName("Read JSON in Spark") \
#     .getOrCreate()
# df_bins = spark.read.json("spark/utils/bins.json")
# df_weather = spark.read.json("spark/utils/weather.json")

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# main_ml(logger,spark, df_bins, df_weather)