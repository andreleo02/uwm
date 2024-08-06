from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable
import logging
import json
import time

class ConnectionException(Exception):
    pass

class Reader:

    def __init__(self, topic):
        self.logger = self.setup_logger()
        self.logger.info("Initializing the consumer")
        self.topic = topic
        while not hasattr(self, 'consumer'):
            self.logger.info("Getting the kafka consumer")
            try:
                self.consumer = KafkaConsumer(bootstrap_servers="kafka:9092",
                                              consumer_timeout_ms=10,
                                              auto_offset_reset='earliest',
                                              group_id=None,
                                              api_version=(3, 7, 0),
                                              value_deserializer = lambda v: json.loads(v.decode('utf-8')))
            except NoBrokersAvailable as err:
                self.logger.error(f"Unable to find a broker: {err}")
                time.sleep(10)

        self.logger.info(f"We have a consumer {time.time()}")
        self.consumer.subscribe(self.topic)
        self.consumer.poll(timeout_ms=10000)
        self.consumer.seek(TopicPartition(topic, 0), 0)

    def setup_logger(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        if len(logger.handlers) == 0:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def next(self):
        """
        Get the "next" event.  This is a pretty naive implementation.  It
        doesn't try to deal with multiple partitions or anything and it assumes
        the event payload is json.
        :return: The event in json form
        """
        try:
            if self.consumer:
                self.logger.debug("A consumer is calling 'next'")
                try:
                    event_partitions = self.consumer.poll(timeout_ms=100,
                                                          max_records=100)
                    event_list = list(event_partitions.values())
                    payload = event_list[0][0]
                    event = payload.value
                    self.logger.info(f'Read an event from the stream {event}')
                    try:
                        return event
                    except json.decoder.JSONDecodeError:
                        return json.loads(f'{{ "message": "{event}" }}')
                except (StopIteration, IndexError):
                    return None
            raise ConnectionException
        except AttributeError as ae:
            self.logger.error("Unable to retrieve the next message.  "
                              "There is no consumer to read from.")
            self.logger.error(str(ae))
            raise ConnectionException