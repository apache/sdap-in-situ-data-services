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
import os

from flask import Blueprint
from flask_restx import Api

from .insitu_query_swagger import api as apidocs
from .cdms_schema import api as cdms_schema_api
from .ingest_json_s3 import api as ingest_parquet_json_s3
from .replace_json_s3 import api as replace_parquet_json_s3
from .query_data import api as query_data
from .query_data_doms import api as query_data_doms
from .extract_statistics_from_parquet_file import api as extract_statistics_from_parquet_file
from .sub_collection_statistics_endpoint import api as sub_collection_statistics_endpoint
from .query_data_doms_custom_pagination import api as query_data_doms_custom_pagination
from ..io_logic.cdms_constants import CDMSConstants

_version = "1.0"
flask_prefix: str = os.environ.get(CDMSConstants.config_key_flask_prefix, '')
flask_prefix = flask_prefix if flask_prefix.startswith('/') else f'/{flask_prefix}'
flask_prefix = flask_prefix if flask_prefix.endswith('/') else f'{flask_prefix}/'

blueprint = Blueprint('parquet_flask', __name__, url_prefix=f'{flask_prefix}{_version}')
blueprint.register_blueprint(apidocs)
api = Api(blueprint,
          title='Parquet ingestion & query',
          version=_version,
          description='API to support the Parquet ingestion & query data',
          doc='/doc/'
          )

# Register namespaces
api.add_namespace(cdms_schema_api)
api.add_namespace(ingest_parquet_json_s3)
api.add_namespace(replace_parquet_json_s3)
api.add_namespace(query_data)
api.add_namespace(query_data_doms)
api.add_namespace(query_data_doms_custom_pagination)
api.add_namespace(extract_statistics_from_parquet_file)
api.add_namespace(sub_collection_statistics_endpoint)
