import time, json, logging
import multiprocessing
from utils.kafka_event_reader import Reader, ConnectionException
from utils.kafka_event_publisher import Publisher
from utils.mongo_utils import save_data_on_mongo
from utils.postgres_utils import insert_bins, insert_weather

MAX_TIMEOUT = 10
MAX_MESSAGES = 100

def process_topic(topic):
    reader = Reader(topic = topic)
    dispatcher = Publisher()
    logging.info(f"Reading data from topic {topic} ...")

    historical_messages = []
    new_messages = []
    while True:
        try:
            message = reader.next()
            if message is not None and message != "":
                historical = message['historical'] == 'True'
                if historical:
                    historical_messages.append(message['sensor_data'])
                else:
                    new_messages.append(message['sensor_data'])
                start_timeout = time.time()
            else:  
                logger.info(f"No new data found for collection {topic}\n")
        except ConnectionException:
            logger.info(json.dumps({
                'status': 'connection_error',
                'message': f'Unable to read from the message stream "{topic}".'}))

        if (len(historical_messages) > 0 and time.time() - start_timeout > MAX_TIMEOUT) or len(historical_messages) >= MAX_MESSAGES:
            save_data_on_mongo(data = historical_messages, collection_name = topic)
            historical_messages = []
            dispatcher.push(topic='export', message={'export_status': 'in_progress'})
        else:
            # this means that the export is completed
            dispatcher.push(topic='export', message={'export_status': 'completed'})

        if (len(new_messages) > 0 and time.time() - start_timeout > MAX_TIMEOUT) or len(new_messages) >= MAX_MESSAGES:
            save_data_on_mongo(data = new_messages, collection_name = topic)
            if topic == "bins":
                insert_bins(bin_data = new_messages)
            elif topic == "weather":
                insert_weather(weather_data = new_messages)
            new_messages = []
 
        time.sleep(1)

if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if len(logger.handlers) == 0:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    jobs = []
    bins = multiprocessing.Process(target=process_topic, args=('bins',))
    weather = multiprocessing.Process(target=process_topic, args=('weather',))
    jobs.append(bins)
    jobs.append(weather)
    bins.start()
    weather.start()