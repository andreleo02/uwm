import schedule
import time
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import UnrecognizedBrokerVersion
import json

# Kafka setup
kafka_bootstrap_servers = ['localhost:9092']
topic = 'ziopera'
group_id = 'my_favorite_group'
try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        api_version=(3, 7, 0)  # Example API version, replace with your broker's version
    )
    print("PRODUCER Successfully connected to Kafka")
except UnrecognizedBrokerVersion:
    print("Unrecognized broker version. Please specify the API version manually.")
    # Specify the API version manually based on your broker's version
    consumer = KafkaConsumer(
        topic = topic,
        group_id='my_favorite_group',
        api_version=(3, 7, 0)  # Example API version, replace with your broker's version
    )
    print("CONSUMER Successfully connected to Kafka")
else:
    
    def read_data_api():
        
        url = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/netvox-r718x-bin-sensor/records?order_by=time%20DESC&limit=2&timezone=Australia%2FMelbourne"

        # Send GET request to API with parameters
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            records = data.get('records', [])
            
            publish_to_kafka(records)
        else:
            print("Error:", response.status_code)


    def publish_to_kafka(data):
        try:
            for record in data:
                producer.send(topic, value=record)
            producer.flush()
            print("Successfully published data to Kafka")
        except Exception as e:
            print("Failed to publish data to Kafka:", e)

    # Schedule read_data_api() to run every 2 minutes
    schedule.every(1).minutes.do(read_data_api)

    # Main loop to run the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)