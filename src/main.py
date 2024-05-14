domain = "https://data.melbourne.vic.gov.au"
all_sensors_resource = "/api/explore/v2.1/catalog/datasets/all-sensors-real-time-status/"

from mongo_utils import get_database
  
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":   
  
   # Get or create the database
   mongo_db = get_database()
