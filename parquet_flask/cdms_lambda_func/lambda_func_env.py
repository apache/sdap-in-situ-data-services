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

class LambdaFuncEnv:
    LOG_LEVEL = 'LOG_LEVEL'
    PARQUET_META_TBL_NAME = 'PARQUET_META_TBL_NAME'
    CDMS_DOMAIN = 'CDMS_DOMAIN'
    CDMS_BEARER_TOKEN = 'CDMS_BEARER_TOKEN'
    SANITIZE_RECORD = 'SANITIZE_RECORD'
    WAIT_TILL_FINISHED = 'WAIT_TILL_FINISHED'
    LOG_FORMAT = '%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
