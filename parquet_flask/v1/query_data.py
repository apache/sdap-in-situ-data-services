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
@api.route('/', methods=["get", "post"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

    @api.expect()
    def get(self):
        return {'message': 'no longer available. Pls use query_data_doms'}, 410

    @api.expect()
    def post(self):
        """
        s3://ecsv-h5-data-v1/INDEX/GALILEO/filenames.txt.gz

        :return:
        """
        return {'message': 'no longer available. Pls use query_data_doms'}, 410
