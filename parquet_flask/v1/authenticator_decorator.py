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

from functools import wraps

from flask import request

from parquet_flask.authenticator.authenticator_abstract import AuthenticatorAbstract
from parquet_flask.authenticator.authenticator_factory import AuthenticatorFactory
from parquet_flask.utils.config import Config


def authenticator_decorator(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        config: Config = Config()
        authenticator: AuthenticatorAbstract = AuthenticatorFactory().get_instance(config.get_value(config.authentication_type, AuthenticatorFactory.FILE))
        try:
            authenticator.get_auth_credentials(config.get_value(config.authentication_key, 'None'))
            auth_result = authenticator.authenticate(request.headers)
            if auth_result is not None:
                return {'message': auth_result}, 403
        except Exception as e:
            return {'message': f'failed while attempting to authenticate', 'details': str(e)}, 500
        return f(*args, **kwargs)
    return decorated_function
