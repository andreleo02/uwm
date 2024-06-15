
from utils.main_postgres import main_spark_postgres
from utils.postgres_utils import PostgreSQLHandler
from pyspark.sql import SparkSession
from utils.main_mongodb import main_spark_mongodb

def main():
    main_spark_postgres() # Activate it to test the PostgreSQL connection
    #main_spark_mongodb() # Activate it to test the MongoDB connection

if __name__ == "__main__":
    main()
