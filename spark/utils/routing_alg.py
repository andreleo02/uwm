
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp, udf
from pyspark.sql.types import DoubleType, StringType
import logging
import numpy as np
from scipy.spatial.distance import pdist, squareform
from scipy.optimize import linear_sum_assignment


def apply_longlag_transformations(logger, df):
    """Convert the column lat_long containing a json to two columns latitude and longitude."""
    
    logger.info("Starting apply_longlag_transformations")
    
    df = df.withColumn("latitude", split(col("lat_long"), ", ")[0].cast(DoubleType()))
    df = df.withColumn("longitude", split(col("lat_long"), ", ")[1].cast(DoubleType()))
    df = df.dropna(subset=["latitude", "longitude"])
    
    logger.info(f"Applied longlag transf with {df.count()} entries")
    return df

def routing_alg(logger, df):
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
    #optimal_distances = dist_matrix[row_ind, col_ind]

    print("Optimal path:", optimal_path)
    # Export optimal path

    # print("Distances on optimal path:", optimal_distances)
    # print("Total distance:", sum(optimal_distances))
    logger.info(f"Applied routing algorithm")

    return optimal_path

def main_routingalg(logger, df_bins):
    """Main function to apply the routing algorithm."""
    logger.info("Starting main_routingalg")
    df_bins = apply_longlag_transformations(logger, df_bins)

    df = df_bins.withColumn("latitude", col("latitude").cast(DoubleType()))
    df = df.withColumn("longitude", col("longitude").cast(DoubleType()))
    df_tsp = df.select("dev_id", "latitude", "longitude")
    #df_tsp.show()

    # Select only the necessary columns for the TSP
    df_tsp = df_bins.select("dev_id", "latitude", "longitude")

    optimal_path = routing_alg(logger, df_tsp)
    return optimal_path

## execution ##
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# spark = SparkSession.builder \
#     .appName("Read JSON in Spark") \
#     .getOrCreate()
# df_bins = spark.read.json("spark/utils/bins.json")
# optimal_path = main_routingalg(logger, df_bins)

