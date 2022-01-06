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
from parquet_flask.v1.ingest_aws_json import IngestAwsJsonProps, IngestAwsJson

api = Namespace('replace_json_s3', description="Ingesting JSON files")
LOGGER = logging.getLogger(__name__)

query_model = api.model('replace_json_s3', {
    's3_url': fields.String(required=True, example='s3://<bucket>/<key>'),
    'job_id': fields.String(required=True, example='sample-uuid'),
})

_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        's3_url': {'type': 'string'},
        'job_id': {'type': 'string'},
    },
    'required': ['s3_url', 'job_id'],
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
        props.s3_url = payload['s3_url']
        props.uuid = payload['job_id']
        props.is_replacing = True
        return IngestAwsJson(props).ingest()
