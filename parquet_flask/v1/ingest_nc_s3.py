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

from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.v1.authenticator_decorator import authenticator_decorator
from parquet_flask.v1.ingest_aws_json import IngestAwsJsonProps
from parquet_flask.v1.ingest_aws_nc import IngestAwsNc

api = Namespace('ingest_nc_s3', description="Ingesting JSON files")
LOGGER = logging.getLogger(__name__)

query_model = api.model('ingest_nc_s3', {
    's3_url': fields.String(required=True, example='s3://<bucket>/<key>'),
    'sanitize_record': fields.Boolean(required=False, example='True', default=True),
    'wait_till_finish': fields.Boolean(required=False, example='True', default=True),
    'observation_key': fields.String(required=True, example='s3://<bucket>/<key>'),
    'lat_key': fields.String(required=True, example='s3://<bucket>/<key>'),
    'lon_key': fields.String(required=True, example='s3://<bucket>/<key>'),
    'time_key': fields.String(required=True, example='s3://<bucket>/<key>'),
    'platform_id_key': fields.String(required=True, example='s3://<bucket>/<key>'),
    'provider': fields.String(required=True, example='s3://<bucket>/<key>'),
    'project': fields.String(required=True, example='s3://<bucket>/<key>'),
    'chunk_size': fields.Integer(required=False, example=1200, default=1200),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        's3_url': {'type': 'string'},
        'sanitize_record': {'type': 'boolean'},
        'wait_till_finish': {'type': 'boolean'},
        'observation_key': {'type': 'string'},
        'lat_key': {'type': 'string'},
        'lon_key': {'type': 'string'},
        'time_key': {'type': 'string'},
        'platform_id_key': {'type': 'string'},
        'provider': {'type': 'string'},
        'project': {'type': 'string'},
        'chunk_size': {'type': 'int'},
    },
    'required': ['s3_url', 'provider', 'project', 'platform_id_key', 'time_key', 'lat_key', 'lon_key', 'observation_key'],
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
        props = IngestAwsJsonProps()
        props.observation_key = payload['observation_key']
        props.lat_key = payload['lat_key']
        props.lon_key = payload['lon_key']
        props.time_key = payload['time_key']
        props.platform_id_key = payload['platform_id_key']
        props.provider = payload['provider']
        props.project = payload['project']
        props.chunk_size = int(payload['chunk_size'] if 'chunk_size' in payload else 1200)
        props.s3_url = payload['s3_url']
        props.is_sanitizing = payload['sanitize_record'] if 'sanitize_record' in payload else True
        props.wait_till_complete = payload['wait_till_finish'] if 'wait_till_finish' in payload else True
        return IngestAwsNc(props).ingest()
