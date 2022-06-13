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

from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.query_v2 import QueryProps, QUERY_PROPS_SCHEMA
from parquet_flask.io_logic.query_v4 import QueryV4
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('query_data_doms_custom_pagination', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('query_data_doms', {
    'itemsPerPage': fields.Integer(required=True, example=0),
    'minDepth': fields.Float(required=True, example=-65.34),
    'maxDepth': fields.Float(required=True, example=-65.34),
    'startTime': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'endTime': fields.String(required=True, example='2020-01-31T00:00:00Z'),
    'markerTime': fields.String(required=False, example='2020-01-02T00:00:00Z', description='timestamp of the last item of the current page'),
    'markerPlatform': fields.String(required=False, example='30', description='platform ID of the last item of the current page'),
    'platform': fields.String(required=True, example='30,3B'),
    'provider': fields.Integer(required=True, example=0),
    'project': fields.Integer(required=True, example=0),
    'columns': fields.String(required=False, example='latitudes, longitudes'),
    'variable': fields.String(required=False, example='air_pressure, relative_humidity'),
    'bbox': fields.String(required=True, example='-45, 175, -30, 180'),  # west, south, east, north
})


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__start_from = 0
        self.__size = 0

    def __get_first_page_url(self):
        new_args = deepcopy(dict(request.args))
        if 'markerTime' in new_args:
            new_args.pop('markerTime')
        if 'markerPlatform' in new_args:
            new_args.pop('markerPlatform')
        new_args = '&'.join([f'{k}={v}' for k, v in new_args.items()])
        return f'{request.base_url}?{new_args}'

    def __get_prev_page_url(self):
        new_args = deepcopy(dict(request.args))
        new_args = '&'.join([f'{k}={v}' for k, v in new_args.items()])
        return f'{request.base_url}?{new_args}'

    def __get_next_page_url(self, query_result: list):
        if len(query_result) < 1:
            return 'NA'
        last_item: dict = query_result[-1]
        new_args = deepcopy(dict(request.args))
        new_args['markerTime'] = last_item[CDMSConstants.time_col]
        new_args['markerPlatform'] = GeneralUtils.gen_sha_256_json_obj(last_item)
        new_args = '&'.join([f'{k}={v}' for k, v in new_args.items()])
        return f'{request.base_url}?{new_args}'

    def __execute_query(self, payload):
        """
        TODO: transform the results to:
        {
            "last": "url",
            "prev": "url",
            "next": "url",
            "first": "url",
            "results": ["results"],
            "total": "number
        }
        :param payload:
        :return:
        """
        is_valid, json_error = GeneralUtils.is_json_valid(payload, QUERY_PROPS_SCHEMA)
        if not is_valid:
            return {'message': 'invalid request body', 'details': str(json_error)}, 400
        try:
            LOGGER.debug(f'<delay_check> query_data_doms_custom_pagination calling QueryV4: {request.args}')
            query = QueryV4(QueryProps().from_json(payload))
            result_set = query.search()
            LOGGER.debug(f'search params: {payload}')
            # page_info = self.__calculate_4_ranges(result_set['total'])
            LOGGER.debug(f'search done')
            result_set['last'] = 'keep browsing next till there is nothing left'
            result_set['first'] = self.__get_first_page_url()
            result_set['prev'] = self.__get_prev_page_url()
            result_set['next'] = self.__get_next_page_url(result_set['results'])
            LOGGER.debug(f'pagination done')
            return result_set, 200
        except Exception as e:
            LOGGER.exception(f'failed to query parquet. cause: {str(e)}')
            return {'message': 'failed to query parquet', 'details': str(e)}, 500

    @api.expect()
    def get(self):
        self.__size = int(request.args.get('itemsPerPage', '10'))
        LOGGER.debug(f'<delay_check> query_data_doms_custom_pagination started: {request.args}')
        query_json = {
            'start_from': self.__start_from,
            'size': self.__size,
        }
        if 'markerPlatform' in request.args:
            query_json['marker_platform_code'] = request.args.get('markerPlatform')

        if 'markerTime' in request.args:
            query_json['min_time'] = request.args.get('markerTime')
        elif 'startTime' in request.args:
            query_json['min_time'] = request.args.get('startTime')
        if 'endTime' in request.args:
            query_json['max_time'] = request.args.get('endTime')

        if 'minDepth' in request.args:
            query_json['min_depth'] = float(request.args.get('minDepth'))
        if 'maxDepth' in request.args:
            query_json['max_depth'] = float(request.args.get('maxDepth'))

        if 'bbox' in request.args:
            bounding_box = GeneralUtils.gen_float_list_from_comma_sep_str(request.args.get('bbox'), 4)
            query_json['min_lat_lon'] = [bounding_box[1], bounding_box[0]]
            query_json['max_lat_lon'] = [bounding_box[3], bounding_box[2]]
        if 'platform' in request.args:
            query_json['platform_code'] = [k.strip() for k in request.args.get('platform').strip().split(',')]
            query_json['platform_code'].sort()
        if 'provider' in request.args:
            query_json['provider'] = request.args.get('provider')
        if 'project' in request.args:
            query_json['project'] = request.args.get('project')
        if 'columns' in request.args and request.args.get('columns').strip() != '':
            query_json['columns'] = [k.strip() for k in request.args.get('columns').split(',')]
        if 'variable' in request.args and request.args.get('variable').strip() != '':
            query_json['variable'] = [k.strip() for k in request.args.get('variable').split(',')]
        return self.__execute_query(query_json)
