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
from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('sub_collection_statistics', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('sub_collection_statistics', {
    'platform': fields.String(required=True, example='30,3B'),
    'provider': fields.Integer(required=True, example=0),
    'project': fields.Integer(required=True, example=0),
})


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class SubCollectionStatisticsEndpoint(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)

    @api.expect()
    def get(self):
        sub_collection_stats_api = SubCollectionStatistics()
        if 'provider' in request.args:
            sub_collection_stats_api.with_provider(request.args.get('provider'))
        if 'project' in request.args:
            sub_collection_stats_api.with_provider(request.args.get('project'))
        if 'platform' in request.args:
            platforms = [k.strip() for k in request.args.get('platform').strip().split(',')]
            platforms.sort()
            sub_collection_stats_api.with_platforms(platforms)

        try:
            sub_collection_stats = sub_collection_stats_api.start()
        except Exception as e:
            LOGGER.exception(f'error while retrieving stats')
            return {'message': 'error while retrieving stats', 'details': str(e)}, 500
        return sub_collection_stats, 200
