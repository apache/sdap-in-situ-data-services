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
from datetime import datetime
from functools import wraps

LOGGER = logging.getLogger(__name__)


def func_exec_time_decorator(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        time1 = datetime.now()
        func_result = f(*args, **kwargs)
        time2 = datetime.now()
        duration = time2 - time1
        LOGGER.info(f'duration: {duration.total_seconds()} s. name: {f.__name__}')
        return func_result, duration.total_seconds(), {'time1': time1.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'time2': time2.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
    return decorated_function
