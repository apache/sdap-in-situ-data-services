# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants
from parquet_flask.io_logic.insitu_records_to_es import InsituRecordsToEs
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.v1.authenticator_decorator import authenticator_decorator

api = Namespace('es_insitu_data', description="Ingesting JSON files")
LOGGER = logging.getLogger(__name__)

query_model = api.model('es_ingest_json_s3', {
    's3_url': fields.String(required=True, example='s3://<bucket>/<key>'),
    'sanitize_record': fields.Boolean(required=False, example='True', default=True),
    'wait_till_finish': fields.Boolean(required=False, example='True', default=True),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        's3_url': {'type': 'string'},
        'sanitize_record': {'type': 'boolean'},
        'wait_till_finish': {'type': 'boolean'},
    },
    'required': ['s3_url'],
}


@api.route('', methods=["put"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)

    @api.expect(fields=query_model)
    @authenticator_decorator
    def put(self):
        """
        s3://ecsv-h5-data-v1/INDEX/GALILEO/filenames.txt.gz

        :return:
        """
        payload = request.get_json()
        is_valid, json_error = GeneralUtils.is_json_valid(payload, _QUERY_SCHEMA)
        if not is_valid:
            return {'message': 'invalid request body', 'details': str(json_error)}, 400
        # props.s3_url = payload["s3_url"]
        # props.is_sanitizing = payload['sanitize_record'] if 'sanitize_record' in payload else True
        # props.wait_till_complete = payload['wait_till_finish'] if 'wait_till_finish' in payload else True
        es_url = os.environ.get(CdmsLambdaConstants.es_url, None)
        try:
            InsituRecordsToEs(es_url).ingest(payload["s3_url"])
        except Exception as e:
            LOGGER.exception(f'deleting error file')
            return {'message': 'failed to ingest to parquet', 'details': str(e)}, 500
        return {'message': 'ingested'}, 200
