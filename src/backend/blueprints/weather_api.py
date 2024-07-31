from flask import Blueprint, jsonify
from utils.postgres_utils import get_weather_details
from utils.redis_utils import get_all_weather, get_weather_status

weather_api = Blueprint('weather_api', __name__)

@weather_api.route('', methods=['GET'])
def get_weathers():
    return jsonify(get_all_weather())

@weather_api.route('/<string:dev_id>/status', methods=['GET'])
def get_weather(dev_id: str):
    return jsonify(get_weather_status(dev_id=dev_id))

@weather_api.route('/<string:dev_id>/details', methods=['GET'])
def get_weather_details_info(dev_id: str):
    weather_details = get_weather_details(dev_id=dev_id)
    weather_status = get_weather_status(dev_id=dev_id)
    result = {}
    if weather_details is not None and weather_status is not None:
        result = {
            'dev_id': dev_id,
            'last_edit': weather_status['last_edit'],
            'battery': weather_status['battery'],
            'precipitation': weather_status['precipitation'],
            'air_temp': weather_status['air_temp'],
            'wind_speed': weather_status['wind_speed'],
            'wind_direction': weather_status['wind_direction'],
            'gust_speed': weather_status['gust_speed'],
            'vapour_pressure': weather_status['vapour_pressure'],
            'atmospheric_pressure': weather_status['atmospheric_pressure'],
            'relative_humidity': weather_status['relative_humidity'],
            'latitude': weather_details.latitude,
            'longitude': weather_details.longitude,
            'sensor_name': weather_details.sensor_name
        }
    return jsonify(result)