import requests
import logging
import pandas as pd
import pytz
import time
import multiprocessing
from urllib import parse
from utils.kafka_event_publisher import Publisher
from utils.pedestrian_data_generator import generate_synthetic_data, get_average_pedestrian_data
from datetime import datetime

def read_data_api(api):
    dispatcher = Publisher()
    melbourne_tz = pytz.timezone('Australia/Melbourne')
    while True:
        results = None
        logger.info(f"Calling API to get new data for collection {api['collection_name']} ...")
        url = api['url'] + parse.quote(f"&where=time>date'{api['last_data']}'", safe="&=-")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
        else:
            logger.error(f"Error getting new data for collection '{api['collection_name']}'. Status code: {response.status_code}")

        if results and len(results) > 0:
            api['last_data'] = results[0]["time"]
            for res in results:
                logger.debug(f"Request had the following data: {res}")
                dispatcher.push(topic=api['collection_name'], message=res)
        else:
            logger.info(f"No new data found for collection '{api['collection_name']}'")
        
        if not api['last_data']:
            api['last_data'] = str(datetime.now(melbourne_tz))

        time.sleep(api['interval'])

def get_pedestrian_data():
    url = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-historical-data/exports/json"
    response = requests.get(url)
    
    if response.status_code == 200:
        try:
            data = response.json()
            df = pd.DataFrame(data)
        except requests.exceptions.JSONDecodeError:
            logger.error("Failed to decode JSON response")
            return
    else:
        logger.error(f"Failed to fetch data. Status code: {response.status_code}")
        return

    avg_visitors_by_hour_and_region = get_average_pedestrian_data(df)

    dispatcher = Publisher()

    while True:
        df = generate_synthetic_data(avg_visitors_by_hour_and_region)
        
        if not df.empty:
            num_rows = 4
            latest_rows = df.head(num_rows)
            
            for _, row in latest_rows.iterrows():
                row_dict = row.to_dict()
                row_dict['dev_id'] = 'ped-c302-3h01'
                dispatcher.push(topic='pedestrian_data', message=row_dict)
        else:
            logger.info(f"No data available to publish to Kafka topic '{'pedestrian_data'}'")

        time.sleep(300)

if __name__ == "__main__":
    limit = 100  # max is 100
    BASE_API = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    API_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"
    API_WEATHER = f"{BASE_API}/meshed-sensor-type-1/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"

    melbourne_tz = pytz.timezone('Australia/Melbourne')

    bins_api = {
        'url': API_BINS,
        'collection_name': "bins",
        'interval': 60,
        'last_data': str(datetime.now(melbourne_tz))
    }

    weather_api = {
        'url': API_WEATHER,
        'collection_name': "weather",
        'interval': 300,
        'last_data': str(datetime.now(melbourne_tz))
    }

    # Set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if len(logger.handlers) == 0:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Start processes
    bins_process = multiprocessing.Process(target=read_data_api, args=(bins_api,))
    weather_process = multiprocessing.Process(target=read_data_api, args=(weather_api,))
    pedestrian_process = multiprocessing.Process(target=get_pedestrian_data)

    jobs = [bins_process, weather_process, pedestrian_process]

    for job in jobs:
        job.start()
