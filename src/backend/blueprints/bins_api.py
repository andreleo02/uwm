
import csv, io
from flask import Blueprint, Response, jsonify
from utils.postgres_utils import get_bin_details
from utils.redis_utils import get_all_bins, get_bin_status, get_optimal_path
from utils.mongo_utils import export_bins
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

@bins_api.route('/export', defaults={'dev_id': None}, methods=['GET'])
@bins_api.route('/export/<string:dev_id>', methods=['GET'])
def export_bins_data(dev_id):
    bins = export_bins(dev_id=dev_id)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=bins[0].keys())
    writer.writeheader()
    for row in bins:
        writer.writerow(row)

    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers.set('Content-Disposition', 'attachment', filename='bins.csv')
    return response

@bins_api.route('/optimal-path', methods=['GET'])
def get_bins_optimal_path():
    optimal_path = get_optimal_path()

    return jsonify(optimal_path)