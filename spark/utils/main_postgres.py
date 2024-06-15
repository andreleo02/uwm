# definire una funzione per tutta questa roba ed importarla in main.py per farlo funzionare!


from pyspark.sql import SparkSession
from utils.postgres_utils import PostgreSQLHandler

def main_spark_postgres():
    """ operations with PostgreSQL
    
    1. Create a table in PostgreSQL database if it does not exist already
    2. Insert data into a PostgreSQL table
    3. Read data from a PostgreSQL table and return a DataFrame
    4. Show DataFrame
    5. Stop SparkSession
    """

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PostgreSQL Example") \
        .getOrCreate()

    # Initialize PostgreSQLHandler instance
    postgres_handler = PostgreSQLHandler(spark)

    # Example usage
    table_name = "my_table"
    schema = "id INT PRIMARY KEY, name VARCHAR(255), city VARCHAR(255), state VARCHAR(255)"
    data = [
        (1, "John Doe", "New York", "NY"),
        (2, "Jane Smith", "San Francisco", "CA"),
        (3, "Michael Johnson", "Chicago", "IL")
    ]
    df = spark.createDataFrame(data, schema="id INT, name STRING, city STRING, state STRING") #forexmaple
    print("DATAFRAME CREATED")
    # Create table if not exists
    postgres_handler.create_table_if_not_exists(table_name, schema)

    # Insert data into PostgreSQL
    postgres_handler.insert_into_table(df, table_name)

    # Read data from PostgreSQL into a DataFrame
    df = postgres_handler.read_from_table(table_name)

    # Show DataFrame
    df.show()

    # Stop SparkSession
    spark.stop()



