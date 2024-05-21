MONGO_PARAMS = "mongodb://root:password"
MONGO_URL = "localhost"

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

def get_or_create_database(database_name: str = "urban_waste") -> Database:
    CONNECTION_STRING = f"{MONGO_PARAMS}@{MONGO_URL}"
    client = MongoClient(CONNECTION_STRING)
    return client[database_name]

def get_or_create_collection(mongo_db: Database, collection_name: str) -> Collection:
    return mongo_db[collection_name]

def insert_data(mongo_collection: Collection, data):
    mongo_collection.insert_many(data)

