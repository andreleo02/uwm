from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class MongoDBHandler:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.MONGO_USER = "root"
        self.MONGO_PASSWORD = "password"
        self.MONGO_HOST = "mongodb"
        self.MONGO_PORT = "27017"
        self.DATABASE_NAME = "urban_waste"
        self.COLLECTION_NAME = "bins"
        self.mongo_uri = f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/?authSource={self.DATABASE_NAME}"
        logger.info(f"Initialized MongoDBHandler with URI: {self.mongo_uri}")

    def read_data(self) -> DataFrame:
        logger.info("Reading data from MongoDB")
        return self.spark.read.format("mongodb") \
            .option("uri", self.mongo_uri) \
            .option("database", self.DATABASE_NAME) \
            .option("collection", self.COLLECTION_NAME) \
            .load()

if __name__ == "__main__":
# def main_spark_mongodb():
    logger.info("Starting SparkSession")
    spark = SparkSession.builder \
        .appName("MongoDB Example") \
        .config("spark.mongodb.read.connection.uri", "mongodb://root:password@mongodb:27017/urban_waste.bins") \
        .config("spark.mongodb.write.connection.uri", "mongodb://root:password@mongodb:27017/urban_waste.bins") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.mongodb:bson:5.1.1,org.mongodb:mongodb-driver-core:5.1.1,org.mongodb:mongodb-driver-sync:5.1.1") \
        .getOrCreate()

    logger.info("SparkSession started")
    
    try:
        mongo_handler = MongoDBHandler(spark)
        mongo_df = mongo_handler.read_data()
        mongo_df.show()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
