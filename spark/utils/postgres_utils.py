import logging
import psycopg2

class PostgreSQLHandler:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.POSTGRES_USER = 'postgres'
        self.POSTGRES_PASSWORD = 'password'
        self.POSTGRES_DB = 'db_waste'
        self.POSTGRES_HOST = 'postgres'
        self.POSTGRES_PORT = '5432'
        self.url = f"jdbc:postgresql://{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

        self.properties = {
            "user": self.POSTGRES_USER,
            "password": self.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

    def create_table_if_not_exists(self, table_name, schema):
        """
        Create a table in PostgreSQL database if it does not exist already
        Args:
            table_name (str): Name of the table to be created
            schema (str): Table schema definition
        """
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
        """
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                dbname=self.POSTGRES_DB,
                user=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT
            )
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"Table {table_name} created successfully")
        except Exception as e:
            logging.error(f"Failed to create table {table_name}: {e}")

    def insert_into_table(self, data_frame, table_name):
        """Insert data into a PostgreSQL table"""
        try:
            data_frame.write.jdbc(url=self.url, table=table_name, mode="append", properties=self.properties)
            logging.info(f"Data inserted successfully into table {table_name}")
        except Exception as e:
            logging.error(f"Failed to insert data into PostgreSQL: {e}")

    def read_from_table(self, table_name):
        """Read data from a PostgreSQL table and return a DataFrame"""
        try:
            df = self.spark.read.jdbc(url=self.url, table=table_name, properties=self.properties)
            return df
        except Exception as e:
            logging.error(f"Failed to read data from PostgreSQL: {e}")
            return None
