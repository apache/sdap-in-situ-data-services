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


api = Namespace('query_data_doms', description="Querying data")
LOGGER = logging.getLogger(__name__)

query_model = api.model('query_data_doms', {
    'startIndex': fields.Integer(required=True, example=0),
    'itemsPerPage': fields.Integer(required=True, example=0),
    'minDepth': fields.Float(required=True, example=-65.34),
    'maxDepth': fields.Float(required=True, example=-65.34),
    'startTime': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'endTime': fields.String(required=True, example='2020-01-31T00:00:00Z'),
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

    @api.expect()
    def get(self):
        return {'message': 'moved permanently to query_data_doms_custom_pagination', 'details': 'deprecated'}, 410
