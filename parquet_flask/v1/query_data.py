import logging

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.query_v2 import QueryProps, Query, QUERY_PROPS_SCHEMA
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('query_data', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('query_data', {
    'provider': fields.String(required=True, example='JPL'),
    'project': fields.String(required=True, example='ABCD'),
    'min_depth': fields.Float(required=True, example=-65.34),
    'max_depth': fields.Float(required=True, example=-65.34),
    'min_time': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'max_time': fields.String(required=True, example='2020-01-31T00:00:00Z'),
    'min_lat_lon': fields.List(fields.Float, required=True, example=[-45, 175]),
    'max_lat_lon': fields.List(fields.Float, required=True, example=[-42.11, 175.16439819335938]),
})


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
