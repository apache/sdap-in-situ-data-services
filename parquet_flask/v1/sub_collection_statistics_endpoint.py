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

from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.utils.time_utils import TimeUtils

api = Namespace('sub_collection_statistics', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('sub_collection_statistics', {
    'minDepth': fields.Float(required=True, example=-65.34),
    'maxDepth': fields.Float(required=True, example=-65.34),
    'startTime': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'endTime': fields.String(required=True, example='2020-01-31T00:00:00Z'),
    'bbox': fields.String(required=True, example='-45, 175, -30, 180', description='west, south, east, north || min_lon, min_lat, max_lon, max_lat'),

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
        try:
            query_props = QueryProps()
            sub_collection_stats_api = SubCollectionStatistics(query_props)
            if 'startTime' in request.args:
                query_props.min_datetime = TimeUtils.get_datetime_obj(request.args.get('startTime')).timestamp()
            if 'endTime' in request.args:
                query_props.max_datetime = TimeUtils.get_datetime_obj(request.args.get('endTime')).timestamp()

            if 'minDepth' in request.args:
                query_props.min_depth = float(request.args.get('minDepth'))
            if 'maxDepth' in request.args:
                query_props.max_depth = float(request.args.get('maxDepth'))

            if 'bbox' in request.args:
                bounding_box = GeneralUtils.gen_float_list_from_comma_sep_str(request.args.get('bbox'), 4)
                query_props.min_lat_lon = [bounding_box[1], bounding_box[0]]
                query_props.max_lat_lon = [bounding_box[3], bounding_box[2]]

            if 'platform' in request.args:
                query_props.platform_code = [k.strip() for k in request.args.get('platform').strip().split(',')]
                query_props.platform_code.sort()
            if 'provider' in request.args:
                query_props.provider = request.args.get('provider')
            if 'project' in request.args:
                query_props.project = request.args.get('project')

            sub_collection_stats = sub_collection_stats_api.start()
        except Exception as e:
            LOGGER.exception(f'error while retrieving stats')
            return {'message': 'error while retrieving stats', 'details': str(e)}, 500
        return sub_collection_stats, 200
