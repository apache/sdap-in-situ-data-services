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

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.ingestion.ingest_props import IngestProps
from parquet_flask.io_logic.ingestion.ingester_core import IngesterCore
from parquet_flask.utils.config import Config
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.v1.authenticator_decorator import authenticator_decorator

api = Namespace('ingest_s3_directory', description="Ingesting JSON files")
LOGGER = logging.getLogger(__name__)

query_model = api.model('ingest_s3_dir', {
    's3_url': fields.String(required=True, example='s3://<bucket>/<key>'),
    'provider': fields.String(required=True, example='AirNow'),
    'project': fields.String(required=True, example='air_quality'),
    'job_id': fields.String(required=True, example='xxx-xxx'),
    'overwrite': fields.Boolean(required=True, example='True'),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        's3_url': {'type': 'string'},
        'job_id': {'type': 'string'},
        'provider': {'type': 'string'},
        'project': {'type': 'string'},
        'overwrite': {'type': 'boolean'},
    },
    'required': ['s3_url', 'provider', 'project', 'job_id', 'overwrite'],
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
        config = Config()
        props = IngestProps()
        props.uuid = payload['job_id']
        props.s3_url = payload["s3_url"]
        props.provider = payload["provider"]
        props.project = payload["project"]
        props.is_replacing = bool(payload["overwrite"])

        props.es_url = config.get_value(Config.es_url)
        props.es_port = int(config.get_value(Config.es_port, '443'))
        props.pub_sub_topic = config.get_value(Config.pub_sub_topic, None)
        props.is_sanitizing = False
        props.wait_till_complete = False

        try:
            IngesterCore(IngesterCore.TYPE_RAW_S3, props).start()
            LOGGER.debug(f'ingestion finished with: {props.result_json, props.result_status_code}')
            return props.result_json, props.result_status_code
        except Exception as e:
            return {'message': 'failed to ingest to parquet', 'details': str(e)}, 500