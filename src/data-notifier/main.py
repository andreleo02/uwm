import time
import requests
import schedule, logging
import datetime
import dateutil.relativedelta
from urllib import parse
from classes.ApiCall import ApiCall
from event_publisher import Publisher
from utils.mongo_utils import is_collection_empty

def read_data_api(api: ApiCall, historical_data: bool = True):
    results = None
    if historical_data:
        if is_collection_empty(api.collection_name()):
            logger.info(f"Calling api to get historical data for collection '{api.collection_name()}' ...")
            now = datetime.datetime.now()
            one_month_ago = now + dateutil.relativedelta.relativedelta(months = -1)
            logger.info(one_month_ago)
            url = api.export_url() + parse.quote(f"&where=time>date'{one_month_ago}'", safe = "&=-")
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                results = data
            else:
                logger.info(f"Error getting historical data for collection '{api.collection_name()}'. Calling url '{api.url()}'", response.status_code)
        else:
            logger.info("Historical data already present. Not calling the export api...")
    else:
        logger.info(f"Calling api to get new data for collection {api.collection_name()} ...")
        url = api.url() + parse.quote(f"&where=time>date'{api.last_data()}'", safe = "&=-")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            results = data["results"] 
        else:
            logger.info(f"Error getting new data for collection '{api.collection_name()}'. Calling url '{api.url()}'", response.status_code)

    if results is not None and len(results) > 0:
        api.set_last_data(results[0]["time"])
        for res in results:
            logger.debug(f"request had the following data: {res}")
            message = {
                'sensor_data': res,
                'historical': f'{historical_data}'
            }
            dispatcher.push(topic = api.collection_name(), message = message)
    else:
        logger.info(f"No new data found for collection '{api.collection_name()}'")
    
    if api.last_data() is None or api.last_data() == '':
        api.set_last_data(str(datetime.datetime.now()))

if __name__ == "__main__":

    limit = 100 # max is 100
    BASE_API = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    API_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit={limit}"
    API_WEATHER = f"{BASE_API}/meshed-sensor-type-1/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"

    EXPORT_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/exports/json?order_by=time%20DESC"
    EXPORT_WEATHER = f"{BASE_API}/meshed-sensor-type-1/exports/json?order_by=time%20DESC"

    bins_api = ApiCall(url = API_BINS, export_url = EXPORT_BINS, collection_name = "bins", interval = 60)
    weather_api = ApiCall(url = API_WEATHER, export_url = EXPORT_WEATHER, collection_name = "weather", interval = 300)
    api_calls = [bins_api, weather_api]

    dispatcher = Publisher()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if len(logger.handlers) == 0:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    for api in api_calls:
        read_data_api(api = api, historical_data = True)
        schedule.every(interval = api.interval()).seconds.do(read_data_api, api, historical_data = False)

    while True:
        schedule.run_pending()
        time.sleep(30)