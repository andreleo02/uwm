from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import logging


class Publisher:

    def __init__(self):
        self.logger = self.setup_logger()
        while not hasattr(self, 'producer'):
            try:
                self.producer = KafkaProducer(bootstrap_servers="kafka:9092", api_version=(3, 7, 0),
                                              value_serializer = lambda v: json.dumps(v).encode('utf-8'))
            except NoBrokersAvailable as err:
                self.logger.error(f"Unable to find a broker: {err}")
                time.sleep(1)

    def setup_logger(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        if len(logger.handlers) == 0:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def push(self, topic, message):
        self.logger.info(f"Publishing: {message}")
        try:
            if self.producer:
                self.producer.send(topic, value = message)
                self.producer.flush()
        except Exception as e:
            self.logger.error(f"Unable to send {message}. The producer does not exist. Exception: {e}")