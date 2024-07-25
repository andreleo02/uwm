############ Machine Learning #############
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, date_format, when, split, col, concat_ws
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
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
    logger.info(f"LATLONG COUNT {df.count()} entries")

    return df

def apply_datetime_transformations(logger, df):
    """
    PARSE TIMESTAMP
    
    """
    
    logger.info("Starting apply_datetime_transformations")
    
    print("TRYING")
    df = df.dropna("all", subset=["time"])
    if 'time' in df.columns:
        if df.schema['time'].dataType.simpleString() == 'struct<$date:string>':
            # Handle time as struct with $date field
            df = df.withColumn("time", to_timestamp(col("time.$date"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
        else:
            # Handle time as string
            df = df.withColumn("time", to_timestamp(col("time"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))

    print("DONE")

    #df = df.withColumn("time", to_timestamp(col("time.$date"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
    #df = df.withColumn("time", to_timestamp(col("time.$date").substr(0, 19), 'yyyy-MM-dd\'T\'HH:mm:ss'))

    # Create separate columns for date and time-only formats as timestamps
    #df = df.withColumn("date", to_timestamp(date_format(col("time"), "yyyy-MM-dd"), 'yyyy-MM-dd'))
    #df = df.withColumn("time_only", to_timestamp(concat_ws(" ", date_format(col("time"), "yyyy-MM-dd"), date_format(col("time"), "HH:mm:ss")), 'yyyy-MM-dd HH:mm:ss'))

    df = df.dropna("all", subset=["time"])

    logger.info(f"Schema after datetime transformations: {df.schema}")
    logger.info(f"Row count after transformations: {df.count()}")
    return df

def main_cleaning(logger, df_bins, df_weather):
    logger.info("Main cleaning")

    #df_bins = df_bins.dropna("any", subset=["lat_long", "time"])
    #df_weather = df_weather.dropna("any", subset=["lat_long", "time"])

    df_bins = apply_datetime_transformations(logger, df_bins)
    df_bins = apply_longlag_transformations(logger, df_bins)
    logger.info(f"df bins LATLONG COUNT {df_bins.count()} entries")

    df_weather = apply_datetime_transformations(logger, df_weather)
    df_weather = apply_longlag_transformations(logger, df_weather)
    
    # Additional cleaning specific to df_weather
    #.drop("battery", "dev_id", "sensor_name", "date", "time_only", "latitude", "longitude")
    df_weather = df_weather.dropDuplicates(["datetime"]).drop("battery", "dev_id", "sensor_name", "date", "time_only", "latitude", "longitude")
    df_bins = df_bins.withColumnRenamed("temperature", "bin_temperature")

    logger.info("Completed cleaning")
    logger.info(f"Final bins schema: {df_bins.schema}")
    logger.info(f"Final weather schema: {df_weather.schema}")

    return df_bins, df_weather


def main_merging(logger, df_bins, df_weather):
    """MERGING WEATHER AND BINS DATAFRAME ON TIMESTAMP"""

    df_merged = df_bins.join(df_weather, "datetime", "left_outer")
    df_merged = df_merged.dropna("any")
    
    return df_merged


def main_ml(logger, spark, df_bins, df_weather):
    bins_clean, weabins_cleanther_clean = main_cleaning(logger, df_bins, df_weather)
    logger.info("Cleaning finished!!")
    df_clean = main_merging(logger, bins_clean, weabins_cleanther_clean)

    logger.info("Merging finished")

    logger.info("Number of rows after cleaning:", df_clean.count())

    feature_cols = ['bin_temperature', 'battery', 'latitude', 'longitude', 'solarpanel', 'solar',
                     'precipitation', 'strikes', 'windspeed', 'vapourpressure', 'atmosphericpressure',
                     'relativehumidity', 'airtemp']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_features = assembler.transform(df_clean)

    train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=1234)

    rf = RandomForestRegressor(featuresCol="features", labelCol="fill_level")
    rf_model = rf.fit(train_data)
    predictions = rf_model.transform(test_data)

    evaluator = RegressionEvaluator(labelCol="fill_level", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

#main_ml(spark, df_bins, df_weather)

########### ziopera  #################

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Read JSON in Spark") \
    .getOrCreate()
df_bins = spark.read.json("spark/utils/bins.json")
#df_bins.show()
print("df_bins schema: ", df_bins.printSchema())
df_weather = spark.read.json("spark/utils/weather.json")
#df_weather.show()
print("df_weather schema: ", df_weather.printSchema())

###################################################################################àà

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# bins = apply_longlag_transformations(logger, df_bins)
# we = apply_longlag_transformations(logger, df_weather)


# df_bins2 = apply_datetime_transformations(logger,bins)
# df_bins2.select("time").show()

# print(df_bins2.printSchema())

#main_ml(logger, spark, df_bins, df_weather)

bins_clean, weabins_cleanther_clean = main_cleaning(logger, df_bins, df_weather)
logger.info("Cleaning finished!!")
print(bins_clean.show())

print(weabins_cleanther_clean.show())
#df_clean = main_merging(logger, bins_clean, weabins_cleanther_clean)
