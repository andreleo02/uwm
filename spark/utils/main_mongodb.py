from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBHandler:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.MONGO_USER = "root"
        self.MONGO_PASSWORD = "password"
        self.MONGO_HOST = "mongodb"
        self.MONGO_PORT = "27017"
        self.DATABASE_NAME = "urban_waste"
        self.COLLECTION_NAME = "bins"
        self.mongo_uri = f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/{self.DATABASE_NAME}"
        logger.info(f"Initialized MongoDBHandler with URI: {self.mongo_uri}")

    def read_data(self) -> DataFrame:
        logger.info("Reading data from MongoDB")
        return self.spark.read.format("mongo").option("uri", self.mongo_uri).option("collection", self.COLLECTION_NAME).load()

# if __name__ == "__main__":
def main_spark_mongodb():
    logger.info("Starting SparkSession")
    spark = SparkSession.builder \
        .appName("MongoDB Example") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1") \
        .config("spark.mongodb.input.uri", "mongodb://root:password@mongodb:27017/urban_waste.bins") \
        .getOrCreate()

    logger.info("SparkSession started")
    
    try:
        mongo_handler = MongoDBHandler(spark)
        mongo_df = mongo_handler.read_data()
        mongo_df.show()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
