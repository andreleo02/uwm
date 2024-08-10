
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp, udf
from pyspark.sql.types import DoubleType, StringType
import logging
import numpy as np
from scipy.spatial.distance import pdist, squareform
from scipy.optimize import linear_sum_assignment
from datetime import datetime, timedelta
import json
import logging
import pytz
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, split, to_timestamp, udf
from pyspark.sql.types import DoubleType, StringType

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def apply_latlong_transformations(df):
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

def apply_datetime_transformations(df):
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

def routing_alg(df):
    """Apply the Hungarian algorithm to find the optimal path."""

    data_list = df.select("dev_id", "latitude", "longitude").collect()
    # Create a dictionary from the collected data for easy access
    data = {row['dev_id']: (row['latitude'], row['longitude']) for row in data_list}

    # Extract coordinates into an array for distance calculation
    coordinates = list(data.values())
    coords_array = np.array(coordinates)
    # Calculate the pairwise Euclidean distance between points
    dist_matrix = squareform(pdist(coords_array, metric='euclidean'))

    # Apply the Hungarian algorithm to find the minimum distance path
    row_ind, col_ind = linear_sum_assignment(dist_matrix)

    # Extract the optimal path and calculate the total distance
    optimal_path = [list(data.keys())[i] for i in col_ind]

    logger.info(f"Applied routing algorithm")

    return optimal_path

def main_routingalg(df_bins):
    """Main function to apply the routing algorithm."""
    logger.info("Starting main_routingalg")
    df_bins = apply_latlong_transformations(df_bins)
    df_bins = apply_datetime_transformations(df_bins)

    utc_now = datetime.now(pytz.utc)
    melbourne_now = utc_now.astimezone(pytz.timezone('Australia/Melbourne'))
    threshold_time_utc = (melbourne_now - timedelta(hours=300)).astimezone(pytz.utc)
    df_bins = df_bins.filter(F.col('time') >= threshold_time_utc)


    df = df_bins.withColumn("latitude", col("latitude").cast(DoubleType()))
    df = df.withColumn("longitude", col("longitude").cast(DoubleType()))
    df_tsp = df.select("dev_id", "latitude", "longitude")
    #df_tsp.show()

    # Select only the necessary columns for the TSP
    df_tsp = df_bins.select("dev_id", "latitude", "longitude")

    optimal_path = routing_alg(df_tsp)
    return optimal_path

# execution LOCALE ##
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# spark = SparkSession.builder \
#     .appName("Read JSON in Spark") \
#     .getOrCreate()
# df_bins = spark.read.json("spark/utils/bins.json")
# optimal_path = main_routingalg(logger, df_bins)

