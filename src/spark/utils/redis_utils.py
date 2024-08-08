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

def save_predictions(latest_weather, grouped_bins):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        bins = [
            {**row.asDict(), "time": row["last(time)"].strftime("%Y-%m-%d %H:%M:%S")}
            for row in grouped_bins
        ]
        for bin in bins:
            logger.info(f"Bin prediction to be saved: {bin}")
            r.hset(f"prediction:bin:{bin['dev_id']}", mapping={
                'fill_level': bin['last(fill_level)'],
                'time': bin['time']
            })
        logger.info("Saved bins prediction data on redis")
    except Exception as e:
        logger.error(f"Error saving bins predictions on redis. Error {e}")

    try:
        weathers = [
            {**row.asDict(), "time": row["time"].strftime("%Y-%m-%d %H:%M:%S")}
            for row in latest_weather
        ]
        for weather in weathers:
            r.hset("prediction:weather", mapping={
                'time': weather['time'],
                'precipitation': weather['precipitation'],
                'strikes': weather['strikes'],
                'windspeed': weather['windspeed'],
                'airtemp': weather['airtemp']
            })
        logger.info("Saved weather prediction data on redis")
    except Exception as e:
        logger.error(f"Error saving weather predictions on redis. Error {e}")

def save_optimal_path(optimal_path):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.lpush("optimal:path", optimal_path)
        logger.info("Saved bins optimal path on redis")
    except Exception as e:
        logger.error(f"Error saving bins optimal path on redis. Error {e}")