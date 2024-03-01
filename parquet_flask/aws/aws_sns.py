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
from parquet_flask.aws.aws_cred import AwsCred

LOGGER = logging.getLogger(__name__)


class AwsSNS(AwsCred):
    def __init__(self):
        super().__init__()
        self.__sns_client = self.get_client('sns')

    def publish(self, topic_arn: str, message: str, subject: str):
        return self.__sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject
        )
