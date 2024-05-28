import time
import requests
import schedule, logging
from urllib import parse
from classes.ApiCall import ApiCall
from event_publisher import Publisher

limit = 100 # max is 100
BASE_API = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
API_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit={limit}"
API_WEATHER = f"{BASE_API}/meshed-sensor-type-1/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"
API_AIR = f"{BASE_API}/argyle-square-air-quality/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"

bins_api = ApiCall(url = API_BINS, collection_name = "bins", interval = 60, save_on_mongo = True, save_on_postgres = False)
weather_api = ApiCall(url = API_WEATHER, collection_name = "weather", interval = 300, save_on_mongo = True, save_on_postgres = False)
air_api = ApiCall(url = API_AIR, collection_name = "air", interval = 600, save_on_mongo = True, save_on_postgres = False)
api_calls = [bins_api, weather_api, air_api]

dispatcher = Publisher()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def read_data_api(api: ApiCall, historical_data: bool = True):
    url = api.url()
    if historical_data:
        logger.info(f"Calling api to get historical data for collection {api.collection_name()} ...")
    else:
        logger.info(f"Calling api to get new data for collection {api.collection_name()} ...")
        url += parse.quote(f"&where=time>date'{api.last_data()}'", safe = "&=-")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["results"] is not None and len(data["results"]) > 0:
            api.set_last_data(data["results"][0]["time"])
            for res in data["results"]:
                logger.debug("request had the following data: {0}".format(res))
                dispatcher.push(topic = api.collection_name(), message = res)
        else:
            logger.info(f"No new data found for collection {api.collection_name()}\n")
    else:
        logger.info(f"Error getting data for collection {api.collection_name()}. \n Calling url {api.url()}", response.status_code)

for api in api_calls:
    read_data_api(api = api, historical_data = True)
    schedule.every(interval = api.interval()).seconds.do(read_data_api, api, historical_data = False)

while True:
    schedule.run_pending()
    time.sleep(30)