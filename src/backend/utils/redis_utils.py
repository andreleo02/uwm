import redis
import logging

REDIS_HOST='redis'
REDIS_PORT=6379

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def get_all_bins():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    keys = r.scan_iter(match='bin:*')
    bins = []
    for key in keys:
        dev_id = str(key).split(':').pop()
        bins.append(get_bin_status(dev_id=dev_id))
    return bins

def get_bin_status(dev_id: str):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    bin_data = r.hgetall(f'bin:{dev_id}')
    bin_data['dev_id'] = dev_id
    return bin_data

def get_all_weather():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    keys = r.scan_iter(match='weather:*')
    weather = []
    for key in keys:
        dev_id = str(key).split(':').pop()
        weather.append(get_weather_status(dev_id=dev_id))
    return weather

def get_weather_status(dev_id: str):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    weather_data = r.hgetall(f'weather:{dev_id}')
    weather_data['dev_id'] = dev_id
    return weather_data

def get_all_pedestrians():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    keys = r.scan_iter(match='pedestrian:*')
    pedestrians = []
    for key in keys:
        splits = str(key).split(':')
        dev_id = splits[1]
        region = splits[2]
        data = get_pedestrian(dev_id=dev_id, region=region)
        pedestrians.append(data)
    return pedestrians

def get_pedestrian(dev_id: str, region: str):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pedestrian = r.hgetall(f'pedestrian:{dev_id}:{region}')
    pedestrian['dev_id'] = dev_id
    return pedestrian