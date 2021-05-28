import json
import logging
import os

from flask_restx import Resource, Namespace, fields
from flask import request
from jsonschema import validate, FormatChecker, ValidationError

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('ingest_json_s3', description="Ingesting JSON files")
LOGGER = logging.getLogger(__name__)

query_model = api.model('ingest_json_s3', {
    's3_url': fields.String(required=True, example='s3://<bucket>/<key>'),
    'time_col': fields.String(required=True, example='time'),
    'partitions': fields.List(fields.String, required=True, example=["project", "provider"]),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        's3_url': {'type': 'string'},
        'time_col': {'type': 'string'},
        'partitions': {
            'type': 'array',
            'items': {
                'type': 'string'
            }
        },
    },
    'required': ['s3_url'],
}


@api.route('', methods=["put"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

    def __get_time_col(self, payload):
        if 'time_col' not in payload:
            return None
        return payload['time_col']

    def __get_partition_list(self, payload):
        if 'partitions' not in payload:
            return []
        return payload['partitions']

    @api.expect(fields=query_model)
    def put(self):
        """
        s3://ecsv-h5-data-v1/INDEX/GALILEO/filenames.txt.gz

        :return:
        """
        payload = request.get_json()
        is_valid, json_error = GeneralUtils.is_json_valid(payload, _QUERY_SCHEMA)
        if not is_valid:
            return {'message': 'invalid request body', 'details': str(json_error)}, 400

        try:
            saved_file_name = AwsS3().download(payload['s3_url'], self.__saved_dir)
            IngestNewJsonFile().ingest(saved_file_name, payload.get('time_col', None), payload.get('partitions', []))
            FileUtils.del_file(saved_file_name)
            return {'message': 'ingested'}, 201
        except Exception as e:
            return {'message': 'failed to ingest to parquest', 'details': str(e)}, 500
