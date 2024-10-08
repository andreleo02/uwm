import csv, io
from flask import Blueprint, Response, jsonify
from utils.postgres_utils import get_weather_details
from utils.redis_utils import get_all_weather, get_weather_status
from utils.mongo_utils import export_weather
from utils.prettifier import prettify_weather_status, prettify_weather_statuses, prettify_weather_details

weather_api = Blueprint('weather_api', __name__)

@weather_api.route('', methods=['GET'])
def get_weathers():
    weather_statuses = get_all_weather()
    result = prettify_weather_statuses(weather_statuses=weather_statuses)
    return jsonify(result)

@weather_api.route('/<string:dev_id>/status', methods=['GET'])
def get_weather(dev_id: str):
    weather_status = get_weather_status(dev_id=dev_id)
    result = prettify_weather_status(weather_status=weather_status)
    return jsonify(result)

@weather_api.route('/<string:dev_id>/details', methods=['GET'])
def get_weather_details_info(dev_id: str):
    weather_details = get_weather_details(dev_id=dev_id)
    weather_status = get_weather_status(dev_id=dev_id)
    result = prettify_weather_details(weather_status=weather_status, weather_details=weather_details)
    return jsonify(result)

@weather_api.route('/export', defaults={'dev_id': None}, methods=['GET'])
@weather_api.route('/export/<string:dev_id>', methods=['GET'])
def export_bins_data(dev_id):
    weather = export_weather(dev_id=dev_id)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=weather[0].keys())
    writer.writeheader()
    for row in weather:
        writer.writerow(row)

    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers.set('Content-Disposition', 'attachment', filename='weather.csv')
    return response