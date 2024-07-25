from flask import Blueprint, jsonify
from utils.postgres_utils import get_all_bins, get_bin_prediction
from marshmallow import Schema, fields

class Decimal(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return None
        return str(value)

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            return Decimal(value)
        except (TypeError, ValueError):
            self.fail("invalid")

class BinSchema(Schema):
    id = fields.Int(dump_only=True)
    dev_id = fields.Str(dump_only=True)
    time = fields.Str(dump_only=True)
    battery = Decimal()
    fill_level = Decimal()
    temperature = Decimal()
    latitude = Decimal()
    longitude = Decimal()

api = Blueprint('api', __name__)

bins_schema = BinSchema(many=True)

@api.route('/bins', methods=['GET'])
def get_bins():
    bins = get_all_bins()
    result = bins_schema.dump(bins)
    return jsonify(result)

@api.route('/<string:dev_id>/prediction', methods=['GET'])
def get_bin_predicted_fill_level(dev_id: str):
    fill_level = get_bin_prediction(dev_id=dev_id)
    return {
        'dev_id': dev_id,
        'fill_level': fill_level
    }