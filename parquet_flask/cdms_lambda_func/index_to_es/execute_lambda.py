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

from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator


def execute_code(event, context):
    os.environ['master_spark_url'] = ''
    os.environ['spark_app_name'] = ''
    os.environ['parquet_file_name'] = ''
    os.environ['in_situ_schema'] = ''
    os.environ['authentication_type'] = ''
    os.environ['authentication_key'] = ''
    os.environ['parquet_metadata_tbl'] = ''
    LambdaLoggerGenerator.remove_default_handlers()

    from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_es_indexer import ParquetFileEsIndexer
    ParquetFileEsIndexer().start(event)
    return
