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
from parquet_flask.io_logic.metadata_tbl_es import MetadataTblES
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics
from parquet_flask.utils.config import Config
from parquet_flask.utils.general_utils import GeneralUtils
from parquet_flask.utils.time_utils import TimeUtils

api = Namespace('query_ingested_file_list', description="Querying data")
LOGGER = logging.getLogger(__name__)
query_model = api.model('query_ingested_file_list', {
    'startTime': fields.String(required=True, example='2020-01-01T00:00:00Z'),
    'endTime': fields.String(required=True, example='2020-01-31T00:00:00Z'),
})


@api.route('', methods=["get"], strict_slashes=False)
@api.route('/', methods=["get"], strict_slashes=False)
class QueryCollectionListEndpoint(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        config = Config()
        es_url = config.get_value(Config.es_url)
        es_port = int(config.get_value(Config.es_port, '443'))
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index='', base_url=es_url, port=es_port)

    @api.expect()
    def get(self):
        try:
            end_time = TimeUtils.get_datetime_obj(request.args.get('endTime')).timestamp() * 1000 if 'endTime' in request.args else None
            start_time = TimeUtils.get_datetime_obj(request.args.get('startTime')).timestamp() * 1000 if 'startTime' in request.args else None
            end_time = end_time if end_time is not None else TimeUtils.get_current_time_unix()
            start_time = start_time if start_time is not None else end_time - 86400000  # 1 day

            metadata_tbl = MetadataTblES(self.__es)
            latest_ingested_files = metadata_tbl.query_by_date_range(start_time, end_time)  # need millisecond unix time
        except Exception as e:
            LOGGER.exception(f'error while retrieving latest_ingested_files')
            return {'message': 'error while retrieving latest_ingested_files', 'details': str(e)}, 500
        return latest_ingested_files, 200
