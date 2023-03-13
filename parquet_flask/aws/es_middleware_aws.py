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

from elasticsearch import Elasticsearch, RequestsHttpConnection

from parquet_flask.aws.aws_cred import AwsCred
from parquet_flask.aws.es_middleware import ESMiddleware
from requests_aws4auth import AWS4Auth

LOGGER = logging.getLogger(__name__)


class EsMiddlewareAws(ESMiddleware):

    def __init__(self, index, base_url: str, port=443) -> None:
        super().__init__(index, base_url, port)
        base_url = base_url.replace('https://', '').replace('http://', '')  # hide https
        base_url = base_url[:-1] if base_url.endswith('/') else base_url
        self._index = index
        aws_cred = AwsCred()
        service = 'es'
        credentials = aws_cred.get_session().get_credentials()
        aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, aws_cred.region, service,
                            session_token=credentials.token)
        self._engine = Elasticsearch(
            hosts=[{'host': base_url, 'port': port}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
