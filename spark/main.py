from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, FloatType, StructField, MapType
from utils.kafka_event_reader import Reader, ConnectionException
import logging, time, json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

bins_struct_type = StructType([
    StructField('battery', FloatType()),
    StructField('dev_id', StringType()),
    StructField('fill_level', FloatType()),
    StructField('lat_long', MapType(StringType(), FloatType())),
    StructField('sensor_name', StringType()),
    StructField('temperature', FloatType()),
    StructField('time', StringType())
])

weather_struct_type = StructType([
    StructField('airtemp', FloatType()),
    StructField('atmosphericpressure', FloatType()),
    StructField('battery', FloatType()),
    StructField('command', FloatType()),
    StructField('dev_id', StringType()),
    StructField('gustspeed', FloatType()),
    StructField('lat_long', MapType(StringType(), FloatType())),
    StructField('precipitation', FloatType()),
    StructField('relativehumidity', FloatType()),
    StructField('rtc', FloatType()),
    StructField('sensor_name', StringType()),
    StructField('solar', FloatType()),
    StructField('solarpanel', FloatType()),
    StructField('strikes', FloatType()),
    StructField('time', StringType()),
    StructField('vapourpressure', FloatType()),
    StructField('winddirection', FloatType()),
    StructField('windspeed', FloatType())
])

class MongoDBHandler:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.MONGO_USER = "root"
        self.MONGO_PASSWORD = "password"
        self.MONGO_HOST = "mongodb"
        self.MONGO_PORT = "27017"
        self.DATABASE_NAME = "urban_waste"
        self.BINS_COLLECTION = "bins"
        self.WEATHER_COLLECTION = "weather"
        self.mongo_uri = f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/{self.DATABASE_NAME}?authSource=admin"
        logger.info(f"Initialized MongoDBHandler with URI: {self.mongo_uri}")

    def read_bins_data(self) -> DataFrame:
        logger.info("Reading bins data from MongoDB")
        return self.spark.read.format("mongodb") \
            .schema(bins_struct_type) \
            .option("uri", self.mongo_uri) \
            .option("database", self.DATABASE_NAME) \
            .option("collection", self.BINS_COLLECTION) \
            .load("mongodb")

    def read_weather_data(self) -> DataFrame:
        logger.info("Reading weather data from MongoDB")
        return self.spark.read.format("mongodb") \
            .schema(weather_struct_type) \
            .option("uri", self.mongo_uri) \
            .option("database", self.DATABASE_NAME) \
            .option("collection", self.WEATHER_COLLECTION) \
            .load("mongodb")
    
    def get_sensors_count(self, df: DataFrame) -> None:
        logger.info("Getting sensors count ...")
        sensors_count = df.select("dev_id").distinct().count()
        logger.info(f"Count of distinct sensors in the df: {sensors_count}")

if __name__ == "__main__":
    logger.info("Starting SparkSession")
    spark = SparkSession.builder \
        .appName("MongoDB Example") \
        .config("spark.mongodb.read.connection.uri", "mongodb://root:password@mongodb:27017/urban_waste?authSource=admin") \
        .config("spark.mongodb.write.connection.uri", "mongodb://root:password@mongodb:27017/urban_waste?authSource=admin") \
        .getOrCreate()

    logger.info("SparkSession started")

    reader = Reader(topic = 'export')
    waiting_data_export = True
    while waiting_data_export:
        message = reader.next()
        if message is not None and message != '':
            waiting_data_export = message['export_status'] == 'in_progress'
            reader = None
        time.sleep(1)
    
    try:
        mongo_handler = MongoDBHandler(spark)
        df_bins = mongo_handler.read_bins_data()
        logger.info(f"Retrieved {df_bins.count()} entries from MongoDB about bins")
        df_weather = mongo_handler.read_weather_data()
        logger.info(f"Retrieved {df_weather.count()} entries from MongoDB about weather")
        mongo_handler.get_sensors_count(df_bins)
        mongo_handler.get_sensors_count(df_weather)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    # do the ml stuff

    while True:
        # update the predictions of the beans on demand
        # getting the demand from Kafka(?) i think is the smartest solution
        time.sleep(30)
