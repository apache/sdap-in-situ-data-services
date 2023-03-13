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
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils

api = Namespace('cdms_schema', description="Retrieve CDMS JSON schema")
LOGGER = logging.getLogger(__name__)


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class CdmsSchema(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)

    @api.expect()
    def get(self):
        json_schema_path = Config().get_value('in_situ_schema')
        if not FileUtils.file_exist(json_schema_path):
            return {'message': f'file not found: {json_schema_path}'}, 404
        json_schema = FileUtils.read_json(json_schema_path)
        if json_schema is None:
            return {'message': 'file is invalid json'}, 500
        return json_schema, 200
