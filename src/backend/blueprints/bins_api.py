from flask import Blueprint, jsonify
from utils.postgres_utils import get_bin_details
from utils.redis_utils import get_all_bins, get_bin_status

bins_api = Blueprint('bins_api', __name__)

@bins_api.route('', methods=['GET'])
def get_bins():
    return jsonify(get_all_bins())

@bins_api.route('/<string:dev_id>/status', methods=['GET'])
def get_bin(dev_id: str):
    return jsonify(get_bin_status(dev_id=dev_id))

@bins_api.route('/<string:dev_id>/details', methods=['GET'])
def get_bin_details_info(dev_id: str):
    bin_details = get_bin_details(dev_id=dev_id)
    bin_status = get_bin_status(dev_id=dev_id)
    result = {}
    if bin_details is not None and bin_status is not None:
        result = {
            'dev_id': dev_id,
            'last_edit': bin_status['last_edit'],
            'battery': bin_status['battery'],
            'fill_level': bin_status['fill_level'],
            'temperature': bin_status['temperature'],
            'latitude': bin_details.latitude,
            'longitude': bin_details.longitude,
            'sensor_name': bin_details.sensor_name
        }
    return jsonify(result)