import logging

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.query import QueryParquet
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('query_data', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('query_data', {
    'depth': fields.Float(required=True, example=-65.34),
    'min_time': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'max_time': fields.String(required=True, example='2020-01-31T00:00:00Z'),
    'min_lat_lon': fields.List(fields.Float, required=True, example=[-45, 175]),
    'max_lat_lon': fields.List(fields.Float, required=True, example=[-42.11, 175.16439819335938]),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        'depth': {'type': 'number'},
        'min_time': {'type': 'string'},
        'max_time': {'type': 'string'},
        'min_lat_lon': {'type': 'array', 'items': {'type': 'number'}, 'minItems': 2, 'maxItems': 2},
        'max_lat_lon': {'type': 'array', 'items': {'type': 'number'}, 'minItems': 2, 'maxItems': 2},
    },
    'required': ['depth', 'min_time', 'max_time', 'min_lat_lon', 'max_lat_lon'],
}


@api.route('', methods=["post"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

    @api.expect()
    def post(self):
        """
        s3://ecsv-h5-data-v1/INDEX/GALILEO/filenames.txt.gz

        :return:
        """
        payload = request.get_json()
        is_valid, json_error = GeneralUtils.is_json_valid(payload, _QUERY_SCHEMA)
        if not is_valid:
            return {'message': 'invalid request body', 'details': str(json_error)}, 400
        try:
            query_parquet = QueryParquet()
            query_parquet.depth = payload['depth']
            query_parquet.min_time = payload['min_time']
            query_parquet.max_time = payload['max_time']
            query_parquet.min_lat_lon = payload['min_lat_lon']
            query_parquet.max_lat_lon = payload['max_lat_lon']
            query_parquet.limit = 1000
            result_set = query_parquet.search()
            for each in result_set:
                if 'time_obj' in each:
                    del each['time_obj']
            LOGGER.debug(f'search params: {payload}. result: {result_set}')
            return {'result_set': result_set}, 200
        except Exception as e:
            LOGGER.exception(f'failed to query parquet. cause: {str(e)}')
            return {'message': 'failed to query parquet', 'details': str(e)}, 500
