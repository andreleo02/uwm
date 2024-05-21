domain = "https://data.melbourne.vic.gov.au"
all_sensors_resource = "/api/explore/v2.1/catalog/datasets/all-sensors-real-time-status/"

from mongo_utils import get_or_create_database, get_or_create_collection, insert_data

import json
  
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":   
  
    # Get or create the database
    mongo_db = get_or_create_database()
    bin_collection = get_or_create_collection(mongo_db = mongo_db, collection_name = "bins")

    # fetch data from opendatasoft API

    file = open("./fakedata.json")
    data = json.load(file)
    file.close()

    insert_data(bin_collection, data)
