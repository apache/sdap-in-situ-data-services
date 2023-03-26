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

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.insitu.get_query_transformer import GetQueryTransformer

from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils
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


class AA(object):

    def __init__(self) -> None:
        super().__init__()
        config = Config()
        self.__file_structure_setting = FileStructureSetting(FileUtils.read_json(config.get_value(Config.in_situ_schema)), FileUtils.read_json(config.get_value(Config.file_structure_setting)))
        self.query_model = None

    def generate_endpoint(self):
        self.__file_structure_setting.get_query_input_transformer_schema()
        return self


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class SubCollectionStatisticsEndpoint(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        config = Config()
        self.__insitu_schema = FileUtils.read_json(config.get_value(Config.in_situ_schema))
        self.__file_structure_setting = FileStructureSetting(self.__insitu_schema, FileUtils.read_json(config.get_value(Config.file_structure_setting)))
        self.__aws_es: ESAbstract = ESFactory().get_instance('AWS', index='',
                                                             base_url=config.get_value(Config.es_url),
                                                             port=int(config.get_value(Config.es_port, '443')))

    @api.expect()
    def get(self):
        try:
            input_params = GetQueryTransformer(self.__file_structure_setting).transform_param(request.args)
            sub_collection_stats_api = SubCollectionStatistics(self.__aws_es, self.__insitu_schema, input_params, self.__file_structure_setting)
            sub_collection_stats = sub_collection_stats_api.start()
        except Exception as e:
            LOGGER.exception(f'error while retrieving stats')
            return {'message': 'error while retrieving stats', 'details': str(e)}, 500
        return sub_collection_stats, 200
