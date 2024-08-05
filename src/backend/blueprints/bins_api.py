from flask import Blueprint, jsonify
from utils.postgres_utils import get_bin_details
from utils.redis_utils import get_all_bins, get_bin_status
from utils.prettifier import prettify_bin_status, prettify_bin_statuses, prettify_bin_details

bins_api = Blueprint('bins_api', __name__)

@bins_api.route('', methods=['GET'])
def get_bins():
    bin_statuses = get_all_bins()
    result = prettify_bin_statuses(bin_statuses=bin_statuses)
    print("Result: ", result)   
    return jsonify(result)

@bins_api.route('/<string:dev_id>/status', methods=['GET'])
def get_bin(dev_id: str):
    bin_status = get_bin_status(dev_id=dev_id)
    result = prettify_bin_status(bin_status=bin_status)
    return jsonify(result)

@bins_api.route('/<string:dev_id>/details', methods=['GET'])
def get_bin_details_info(dev_id: str):
    bin_status = get_bin_status(dev_id=dev_id)
    bin_details = get_bin_details(dev_id=dev_id)
    result = prettify_bin_details(bin_status=bin_status, bin_details=bin_details)
    return jsonify(result)