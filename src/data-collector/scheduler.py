import time
import requests

from mongo_utils import save_data_on_mongo
from postgres_utils import save_data_on_postgres
from schedule import every, run_pending
import requests

limit = 50 # max is 100
BASE_API = "https://data.melbourne.vic.gov.au//api/explore/v2.1/catalog/datasets"
API_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"
API_WEATHER = f"{BASE_API}/datasets/meshed-sensor-type-1/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"
API_AIR = f"{BASE_API}/argyle-square-air-quality/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"

api_calls = [
    {
        "collection_name": "bins",
        "url": API_BINS,
        "interval": 1
    },
    {
        "collection_name": "weather",
        "url": API_WEATHER,
        "interval": 5
    },
    {
        "collection_name": "air",
        "url": API_AIR,
        "interval": 10
    }
]

def read_data_api(url: str,  collection_name: str, save_on_mongo: bool = True, save_on_postgres: bool = True):
    response = requests.get(url)
  
    if response.status_code == 200:
        data = response.json()
        if save_on_mongo:
            save_data_on_mongo(data = data["results"], collection_name = collection_name)
            print("Data saved on mongo!")
        if save_on_postgres:
            save_data_on_postgres(data = data["results"])
    else:
        print("Error: ", response.status_code)

for api_call in api_calls:
    collection_name = api_call["collection_name"]
    url = api_call["url"]
    interval = api_call["interval"]
    every(interval = interval).minutes.do(read_data_api, url = url, save_on_postgres = False, collection_name = collection_name)

while True:
    run_pending()
    time.sleep(40)