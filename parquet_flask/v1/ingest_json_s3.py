import json
import logging
import os
import uuid

from flask_restx import Resource, Namespace, fields
from flask import request
from jsonschema import validate, FormatChecker, ValidationError

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.utils.time_utils import TimeUtils

api = Namespace('ingest_json_s3', description="Ingesting JSON files")
LOGGER = logging.getLogger(__name__)

query_model = api.model('ingest_json_s3', {
    's3_url': fields.String(required=True, example='s3://<bucket>/<key>'),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        's3_url': {'type': 'string'},
    },
    'required': ['s3_url'],
}


@api.route('', methods=["put"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

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
            LOGGER.debug(f'starting to ingest: {payload["s3_url"]}')
            s3 = AwsS3().set_s3_url(payload['s3_url'])
            job_id = str(uuid.uuid4())
            LOGGER.debug(f'downloading s3 file: {job_id}')
            saved_file_name = AwsS3().download( self.__saved_dir, payload['s3_url'])
            LOGGER.debug(f'ingesting')
            IngestNewJsonFile().ingest(saved_file_name, job_id)
            LOGGER.debug(f'deleting used file')
            FileUtils.del_file(saved_file_name)
            # TODO make it background process?
            # TODO store job details in database
            # TODO add these: job start time, job end time, validation metadata, s3 url, job id
            LOGGER.debug(f'tagging s3')
            s3.add_tags_to_obj({
                'parquet_ingested': TimeUtils.get_current_time_str(),
                'job_id': job_id,
            })
            return {'message': 'ingested'}, 201
        except Exception as e:
            return {'message': 'failed to ingest to parquet', 'details': str(e)}, 500
