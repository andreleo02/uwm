import psycopg2
import pandas as pd

# Database connection parameters
user = 'postgres'
password = 'password'
host = 'localhost'
database = 'db_waste'

# Establishing the connection
conn = psycopg2.connect(user = user, password = password, host = host, database = database)

# Function to create table
def create_table(query):
    """Function to create a table"""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()

# Example usage
# q = """
# CREATE TABLE titanic (
#     survived BOOLEAN,  -- assuming 'survived' is a boolean (True/False)
#     pclass INTEGER     -- assuming 'pclass' is an integer
# )
# """
# create_table(q)

def read_sql(query):
    """Function to read from a table"""
    df = pd.read_sql(query, conn)
    return df

# Example usage
# query = "SELECT * FROM titanic"
# df = read_sql(query)

# Function to insert values into table
def insert_values(query):
    """Function to write into a table"""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()

# Example usage
# query = "INSERT INTO titanic (survived, pclass) VALUES (True, 1);"
# insert_values(query)

# Close the connection when done
def close_connection():
    """Function to close the database connection"""
    if conn:
        conn.close()


def save_data_on_postgres(data):
    return
# Close connection example
# close_connection()
   