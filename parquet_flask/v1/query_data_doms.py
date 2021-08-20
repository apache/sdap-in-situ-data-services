import json
import logging

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.query_v2 import QueryProps, Query, QUERY_PROPS_SCHEMA
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('query_data_doms', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('query_data_doms', {
    'startIndex': fields.Integer(required=True, example=0),
    'itemsPerPage': fields.Integer(required=True, example=0),
    'minDepth': fields.Float(required=True, example=-65.34),
    'maxDepth': fields.Float(required=True, example=-65.34),
    'startTime': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'endTime': fields.String(required=True, example='2020-01-31T00:00:00Z'),
    'platform': fields.Integer(required=True, example=0),
    'columns': fields.List(fields.String, required=False, example=['latitudes', 'longitudes']),
    'bbox': fields.List(fields.Float, required=True, example=[-45, 175, -30, 180]),  # west, south, east, north
})


@api.route('', methods=["get", "post"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

    def __execute_query(self, payload):
        is_valid, json_error = GeneralUtils.is_json_valid(payload, QUERY_PROPS_SCHEMA)
        if not is_valid:
            return {'message': 'invalid request body', 'details': str(json_error)}, 400
        try:
            query = Query(QueryProps().from_json(payload))
            result_set = query.search()
            LOGGER.debug(f'search params: {payload}. result: {result_set}')
            return {'result_set': result_set}, 200
        except Exception as e:
            LOGGER.exception(f'failed to query parquet. cause: {str(e)}')
            return {'message': 'failed to query parquet', 'details': str(e)}, 500

    @api.expect()
    def get(self):
        query_json = {
            'start_from': int(request.args.get('startIndex', '0')),
            'size': int(request.args.get('itemsPerPage', '10')),
        }
        if 'startTime' in request.args:
            query_json['min_time'] = request.args.get('startTime')
        if 'endTime' in request.args:
            query_json['max_time'] = request.args.get('endTime')
        if 'minDepth' in request.args:
            query_json['min_depth'] = float(request.args.get('minDepth'))
        if 'maxDepth' in request.args:
            query_json['max_depth'] = float(request.args.get('maxDepth'))
        if 'bbox' in request.args:
            bounding_box = json.loads(request.args.get('bbox'))
            query_json['min_lat_lon'] = [bounding_box[1], bounding_box[0]]
            query_json['max_lat_lon'] = [bounding_box[3], bounding_box[2]]
        return self.__execute_query(query_json)
