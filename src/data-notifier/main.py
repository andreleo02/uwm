import requests
import logging
import pandas as pd
import numpy as np
from datetime import timedelta, datetime

import pytz
import time
import multiprocessing
from urllib import parse
from utils.kafka_event_publisher import Publisher



def read_data_api(api):
    dispatcher = Publisher()
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
            api['last_data'] = str(datetime.now())

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

    df = clean_data(df)
    
    if not df.empty:
        publish_to_kafka(df, 'pedestrian_data')

def clean_data(df):
    if df.empty:
        return pd.DataFrame()

    # Convert datetime column to datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

    # Filter out negative visitors and select necessary columns
    filtered_data = df[df['numvisitors'] >= 0][['datetime', 'region', 'numvisitors']]

    # Convert to Melbourne timezone
    melbourne_tz = pytz.timezone('Australia/Melbourne')
    filtered_data['datetime'] = filtered_data['datetime'].dt.tz_convert('UTC').dt.tz_convert(melbourne_tz)

    # Filter data up to June 30, 2021
    filtered_data = filtered_data[filtered_data['datetime'] <= '2021-06-30']

    # Calculate average visitors by hour and region
    avg_visitors_by_hour_and_region = filtered_data.groupby([filtered_data['region'], filtered_data['datetime'].dt.hour])['numvisitors'].mean().unstack()

    # Define the date range for synthetic data
    synthetic_start_date = melbourne_tz.localize(datetime(2024, 3, 1, 0, 0, 0))
    current_date = datetime.now(melbourne_tz)

    # Create synthetic data using Poisson distribution
    regions = avg_visitors_by_hour_and_region.index
    synthetic_data = []
    current = synthetic_start_date

    while current <= current_date:
        hour = current.hour
        for region in regions:
            lam = avg_visitors_by_hour_and_region.loc[region, hour] if hour in avg_visitors_by_hour_and_region.columns else 0
            if lam > 0:
                num_visitors = np.random.poisson(lam)
                synthetic_data.append({
                    'datetime': current.isoformat(),
                    'region': region,
                    'numVisitors': num_visitors
                })
        current += timedelta(hours=1)

    # Create DataFrame from synthetic data
    synthetic_df = pd.DataFrame(synthetic_data)
    synthetic_df = synthetic_df.sort_values(by='datetime', ascending=False)
    
    return synthetic_df

def publish_to_kafka(df, topic):
    dispatcher = Publisher()  # Assuming Publisher is your Kafka publisher class
    
    while True:
        if not df.empty:
            
            num_rows = 4
            latest_rows = df.head(num_rows)
            
            for _, row in latest_rows.iterrows():
                row_dict = row.to_dict()
                logger.info(f"Publishing data to Kafka topic '{topic}': {row_dict}")
                dispatcher.push(topic=topic, message=row_dict)
        else:
            logger.info(f"No data available to publish to Kafka topic '{topic}'")
        
        # Wait for 5 minutes before sending the next batch of messages
        time.sleep(300)  # 300 seconds = 5 minutes

if __name__ == "__main__":
    limit = 100  # max is 100
    BASE_API = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    API_BINS = f"{BASE_API}/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit={limit}"
    API_WEATHER = f"{BASE_API}/meshed-sensor-type-1/records?order_by=time%20DESC&limit={limit}&timezone=Australia%2FMelbourne"

    bins_api = {
        'url': API_BINS,
        'collection_name': "bins",
        'interval': 60,
        'last_data': str(datetime.now())
    }

    weather_api = {
        'url': API_WEATHER,
        'collection_name': "weather",
        'interval': 300,
        'last_data': str(datetime.now())
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
    
    for job in jobs:
        job.join()
