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
from copy import deepcopy

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.query_v2 import QueryProps, QUERY_PROPS_SCHEMA
from parquet_flask.io_logic.query_v4 import QueryV4
from parquet_flask.io_logic.statistics_retriever_wrapper import StatisticsRetrieverWrapper
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('extract_stats', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('extract_stats', {
    's3_key': fields.String(required=True, example=''),
})


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)

    @api.expect()
    def get(self):
        s3_key = request.args.get('s3_key', '')
        if s3_key == '':
            return {'message': 'invalid input. must have s3_key'}, 500
        try:
            parquet_stats = StatisticsRetrieverWrapper().start(s3_key)
        except Exception as e:
            LOGGER.exception(f'error while retrieving stats for s3_key: {s3_key}')
            return {'message': 'error while retrieving stats', 'details': str(e)}, 500
        return parquet_stats, 200
