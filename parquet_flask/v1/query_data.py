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

import json
import logging

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.query_v2 import QueryProps, Query, QUERY_PROPS_SCHEMA
from parquet_flask.io_logic.query_v3 import QueryV3
from parquet_flask.utils.general_utils import GeneralUtils

api = Namespace('query_data', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('query_data', {
    'start_from': fields.Integer(required=True, example=0),
    'size': fields.Integer(required=True, example=0),
    'provider': fields.String(required=True, example='JPL'),
    'project': fields.String(required=True, example='ABCD'),
    'min_depth': fields.Float(required=True, example=-65.34),
    'max_depth': fields.Float(required=True, example=-65.34),
    'min_time': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'max_time': fields.String(required=True, example='2020-01-31T00:00:00Z'),
    'columns': fields.String(required=False, example='latitudes, longitudes'),
    'variable': fields.String(required=False, example='air_pressure, relative_humidity'),
    'min_lat_lon': fields.String(required=True, example='-45, 175'),
    'max_lat_lon': fields.String(required=True, example='-42.11, 175.16439819335938'),
})


@api.route('', methods=["get", "post"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

    def __execute_query(self, payload):
        is_valid, json_error = GeneralUtils.is_json_valid(payload, QUERY_PROPS_SCHEMA)
        if not is_valid:
            return {'message': 'invalid request body', 'details': str(json_error)}, 400
        try:
            query = QueryV3(QueryProps().from_json(payload))
            result_set = query.search()
            LOGGER.debug(f'search params: {payload}. result: {result_set}')
            return {'result_set': result_set}, 200
        except Exception as e:
            LOGGER.exception(f'failed to query parquet. cause: {str(e)}')
            return {'message': 'failed to query parquet', 'details': str(e)}, 500

    @api.expect()
    def get(self):
        query_json = {
            'start_from': request.args.get('start_from', '0'),
            'size': request.args.get('size', '10'),
        }
        if 'min_time' in request.args:
            query_json['min_time'] = request.args.get('min_time')
        if 'max_time' in request.args:
            query_json['max_time'] = request.args.get('max_time')
        if 'min_depth' in request.args:
            query_json['min_depth'] = float(request.args.get('min_depth'))
        if 'max_depth' in request.args:
            query_json['max_depth'] = float(request.args.get('max_depth'))
        if 'columns' in request.args:
            query_json['columns'] = [k.strip() for k in request.args.get('columns').split(',')]
        if 'variable' in request.args:
            query_json['variable'] = [k.strip() for k in request.args.get('variable').split(',')]
        if 'min_lat_lon' in request.args:
            query_json['min_lat_lon'] = GeneralUtils.gen_float_list_from_comma_sep_str(request.args.get('min_lat_lon'), 2)
        if 'max_lat_lon' in request.args:
            query_json['max_lat_lon'] = GeneralUtils.gen_float_list_from_comma_sep_str(request.args.get('max_lat_lon'), 2)
        return self.__execute_query(query_json)

    @api.expect()
    def post(self):
        """
        s3://ecsv-h5-data-v1/INDEX/GALILEO/filenames.txt.gz

        :return:
        """
        return self.__execute_query(request.get_json())
