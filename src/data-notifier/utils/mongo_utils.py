MONGO_PARAMS = "mongodb://root:password"
MONGO_URL = "mongodb:27017"

import logging
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def get_or_create_database(database_name: str = "urban_waste") -> Database:
    CONNECTION_STRING = f"{MONGO_PARAMS}@{MONGO_URL}"
    client = MongoClient(CONNECTION_STRING)
    return client[database_name]

def get_or_create_collection(mongo_db: Database, collection_name: str) -> Collection:
    return mongo_db[collection_name]

def is_collection_empty(collection_name):
    mongo_db = get_or_create_database()
    collection = get_or_create_collection(mongo_db = mongo_db, collection_name = collection_name)
    return collection.count_documents({}) == 0
