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

BINS_COLLECTION = 'bins'
WEATHER_COLLECTION = 'weather'

def get_or_create_database(database_name: str = "urban_waste") -> Database:
    CONNECTION_STRING = f"{MONGO_PARAMS}@{MONGO_URL}"
    client = MongoClient(CONNECTION_STRING)
    return client[database_name]

def get_or_create_collection(mongo_db: Database, collection_name: str) -> Collection:
    return mongo_db[collection_name]

def export_bins(dev_id):
    mongo_db = get_or_create_database()
    collection = get_or_create_collection(mongo_db = mongo_db, collection_name = BINS_COLLECTION)
    bins = []
    try:
        if dev_id is None:
            bins = list(collection.find({}, {'_id': 0, 'fill_level': 0}))
        else:
            filter = {'dev_id': dev_id}
            bins = list(collection.find(filter, {'_id': 0, 'fill_level': 0}))
    except Exception as e:
        logger.info(f"Error querying all bins data from MongoDB. Exception: {e}")
    logger.info(f"Retrieved from MongoDB {len(bins)} data")
    return bins

def export_weather(dev_id):
    mongo_db = get_or_create_database()
    collection = get_or_create_collection(mongo_db = mongo_db, collection_name = WEATHER_COLLECTION)
    weather = []
    try:
        if dev_id is None:
            weather = list(collection.find({}, {'_id': 0, 'fill_level': 0}))
        else:
            filter = {'dev_id': dev_id}
            weather = list(collection.find(filter, {'_id': 0, 'fill_level': 0}))
    except Exception as e:
        logger.info(f"Error querying all weather data from MongoDB. Exception: {e}")
    logger.info(f"Retrieved from MongoDB {len(weather)} data")
    return weather