import requests
import logging
import datetime, time
import multiprocessing
from urllib import parse
from utils.kafka_event_publisher import Publisher

def read_data_api(api):
    dispatcher = Publisher()
    while True:
        results = None
        logger.info(f"Calling api to get new data for collection {api['collection_name']} ...")
        url = api['url'] + parse.quote(f"&where=time>date'{api['last_data']}'", safe = "&=-")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            results = data["results"] 
        else:
            logger.info(f"Error getting new data for collection '{api['collection_name']}'. Calling url '{api['url']}'", response.status_code)

        if results is not None and len(results) > 0:
            api['last_data'] = results[0]["time"]
            for res in results:
                logger.debug(f"request had the following data: {res}")
                dispatcher.push(topic = api['collection_name'], message = res)
        else:
            logger.info(f"No new data found for collection '{api['collection_name']}'")
        
        if api['last_data'] is None or api['last_data'] == '':
            api.set_last_data(str(datetime.datetime.now()))

        time.sleep(api['interval'])

if __name__ == "__main__":

    limit = 100 # max is 100
    BASE_API = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    API_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"
    API_WEATHER = f"{BASE_API}/meshed-sensor-type-1/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"

    bins_api = {
        'url': API_BINS,
        'collection_name': "bins",
        'interval': 60,
        'last_data':str(datetime.datetime.now())
    }

    weather_api = {
        'url': API_WEATHER,
        'collection_name': "weather",
        'interval': 300,
        'last_data': str(datetime.datetime.now())
    }

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if len(logger.handlers) == 0:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    bins = multiprocessing.Process(target=read_data_api, args=(bins_api,))
    weather = multiprocessing.Process(target=read_data_api, args=(weather_api,))
    jobs = []
    jobs.append(bins)
    jobs.append(weather)
    bins.start()
    weather.start()