MONGO_PARAMS = "mongodb://root:password"
MONGO_URL = "localhost"

from pymongo import MongoClient
def get_database():
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = f"{MONGO_PARAMS}@{MONGO_URL}/BigDataMongo"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['pattumiere']

def load_data(data):
   # load data into mongo
   return True
