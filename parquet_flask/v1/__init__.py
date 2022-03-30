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

from flask import Blueprint
from flask_restx import Api
from .insitu_query_swagger import api as apidocs
from .ingest_json_s3 import api as ingest_parquet_json_s3
from .replace_json_s3 import api as replace_parquet_json_s3
from .query_data import api as query_data
from .query_data_doms import api as query_data_doms
_version = "1.0"

blueprint = Blueprint('parquet_flask', __name__, url_prefix='/{}'.format(_version))
blueprint.register_blueprint(apidocs)
api = Api(blueprint,
          title='Parquet ingestion & query',
          version=_version,
          description='API to support the Parquet ingestion & query data',
          doc='/doc/'
          )

# Register namespaces
api.add_namespace(ingest_parquet_json_s3)
api.add_namespace(replace_parquet_json_s3)
api.add_namespace(query_data)
api.add_namespace(query_data_doms)
