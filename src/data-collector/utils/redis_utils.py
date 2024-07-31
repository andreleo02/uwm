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

def update_bins_statuses(bin_data):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    for bin in bin_data:
        r.hset(f"bin:{bin['dev_id']}", mapping={
            'battery': bin['battery'],
            'fill_level': bin['filllevel'],
            'last_edit': bin['time'],
            'temperature': bin['temperature']
        })
    logger.info(f"Saved {len(bin_data)} bins data on redis")

def update_weather_statuses(weather_data):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    for weather in weather_data:
        r.hset(f"weather:{weather['dev_id']}", mapping={
            'last_edit': weather['time'],
            'battery': weather['battery'],
            'precipitation': weather['precipitation'],
            'air_temp': weather['airtemp'],
            'wind_speed': weather['windspeed'],
            'wind_direction': weather['winddirection'],
            'gust_speed': weather['gustspeed'],
            'vapour_pressure': weather['vapourpressure'],
            'atmospheric_pressure': weather['atmosphericpressure'],
            'relative_humidity': weather['relativehumidity']
        })
    logger.info(f"Saved {len(weather_data)} weather data on redis")