import logging
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

MONGO_PARAMS = "mongodb://root:password"
MONGO_URL = "mongodb"

def get_or_create_database(database_name: str = "urban_waste") -> Database:
    CONNECTION_STRING = f"{MONGO_PARAMS}@{MONGO_URL}"
    try:
        client = MongoClient(CONNECTION_STRING)
        return client[database_name]
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise

def get_or_create_collection(mongo_db: Database, collection_name: str) -> Collection:
    try:
        return mongo_db[collection_name]
    except Exception as e:
        logger.error(f"Error getting collection: {e}")
        raise

def insert_data(mongo_collection: Collection, data):
    #if isinstance(data, list) and data:
    try:
        mongo_collection.insert_many(data)
        logger.info("Data successfully inserted.")
    except Exception as e:
        logger.error(f"Error inserting data into MongoDB: {e}")
    else:
        logger.warning("Data must be a non-empty list. No data inserted.")

def save_data_on_mongo(data, collection_name):
    try:
        mongo_db = get_or_create_database()
        collection = get_or_create_collection(mongo_db, collection_name)
        insert_data(collection, data)
    except Exception as e:
        logger.error(f"Error saving data on MongoDB: {e}")


# Example data to be inserted
# sample_data = [
#     {"name": "Plastic Bottle", "type": "Plastic", "quantity": 100},
#     {"name": "Cardboard Box", "type": "Paper", "quantity": 50},
#     {"name": "Aluminum Can", "type": "Metal", "quantity": 200}
# ]

# # Save the example data into the 'waste_collection' collection
# save_data_on_mongo(sample_data, "waste_collection")