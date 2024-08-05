from flask import Blueprint, jsonify
from utils.redis_utils import get_all_pedestrians
from utils.prettifier import prettify_pedestrians

pedestrians_api = Blueprint('pedestrian_api', __name__)

@pedestrians_api.route('', methods=['GET'])
def get_pedestrians():
    pedestrians = get_all_pedestrians()
    result = prettify_pedestrians(pedestrians=pedestrians)
    return jsonify(result)