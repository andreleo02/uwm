from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import os
import time
import logging

topic = os.environ.get('PCDEMO_CHANNEL') or 'new_data_topic'


class Publisher:

    def __init__(self):
        self.logger = self.setup_logger()
        while not hasattr(self, 'producer'):
            try:
                self.producer = KafkaProducer(bootstrap_servers="kafka:9092", api_version=(3, 7, 0),
                                              value_serializer = lambda v: json.dumps(v).encode('utf-8')
                                              )
            except NoBrokersAvailable as err:
                self.logger.error("Unable to find a broker: {0}".format(err))
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

    def push(self, message):
        self.logger.info("Publishing: {0}".format(message))
        try:
            if self.producer:
                self.producer.send(topic, value = message)
                self.producer.flush()
        except Exception:
            self.logger.error("Unable to send {0}. The producer does not exist."
                              .format(message))