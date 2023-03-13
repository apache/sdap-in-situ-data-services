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

from datetime import datetime, timezone


class TimeUtils:
    @staticmethod
    def get_current_time_str() -> str:
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    @staticmethod
    def get_current_time_unix() -> int:
        return int(datetime.utcnow().timestamp() * 1000)

    @staticmethod
    def get_datetime_obj(dt_str, fmt='%Y-%m-%dT%H:%M:%SZ') -> datetime:
        return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)

    @staticmethod
    def get_time_str(unix_timestamp, fmt='%Y-%m-%dT%H:%M:%SZ', in_ms=True) -> str:
        converting_timestamp = unix_timestamp / 1000 if in_ms is True else unix_timestamp
        return datetime.utcfromtimestamp(converting_timestamp).strftime(fmt)
