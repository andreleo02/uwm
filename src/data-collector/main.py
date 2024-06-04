import threading, time, json, logging
from event_reader import Reader, ConnectionException
from utils.mongo_utils import save_data_on_mongo

def process_topic(topic):
    reader = Reader(topic = topic)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.info(f"Reading data from topic {topic} ...")

    if len(logger.handlers) == 0:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    while True:
        time.sleep(1)
        try:
            message = reader.next()
            if message is not None or message != "":    
                message = json.dumps(message)
                logger.debug(f"Message received: {message}")
                message = json.loads(message)
                message = [message]
                if len(message) > 0:
                    save_data_on_mongo(data = message, collection_name = topic)
                    logger.info("Data saved on mongo.")
            else:  
                print(f"No new data found for collection {topic}\n")
        except ConnectionException:
            logger.info(json.dumps({
                'status': 'connection_error',
                'message': 'Unable to read from the message stream.'}))

        logger.info(f"Read this data from the stream: {message}")

if __name__ == "__main__":
    topics = ['air', 'bins', 'weather']

    for topic in topics:
        threading.Thread(target = process_topic, args = (topic, )).start()