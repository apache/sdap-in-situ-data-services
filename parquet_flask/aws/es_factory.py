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

from parquet_flask.utils.factory_abstract import FactoryAbstract


class ESFactory(FactoryAbstract):
    NO_AUTH = 'NO_AUTH'
    AWS = 'AWS'

    def get_instance(self, class_type, **kwargs):
        ct = class_type.upper()
        if ct == self.NO_AUTH:
            from parquet_flask.aws.es_middleware import ESMiddleware
            return ESMiddleware(kwargs['index'], kwargs['base_url'], port=kwargs['port'])
        if ct == self.AWS:
            from parquet_flask.aws.es_middleware_aws import EsMiddlewareAws
            return EsMiddlewareAws(kwargs['index'], kwargs['base_url'], port=kwargs['port'])
        raise ModuleNotFoundError(f'cannot find ES class for {ct}')
