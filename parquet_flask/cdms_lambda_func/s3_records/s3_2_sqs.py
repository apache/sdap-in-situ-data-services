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

"""
{
  "Records": [
    {
      "messageId": "6210f778-d081-4ae9-a861-8534d612dfae",
      "receiptHandle": "AQEBk55DchogyQzpVsH1A4YEj4K/PcVuIG9Em/a6/4AHIA4G5vLPiHVElNiuMfYc1ussk2U//JwZbD788Fv8u6W22L3AJ1U8EIcGJ57aibpmd6tSCWLS5q5FA4u2X2Jq5z+lCX5NZXzNDYMqMJaCGtBkcYi4a9LDXtD+U7HWX0V8OPhFFF2a1qUu+E05c16f5OmE7wRJ3SFrRmtJOhp2DigKKsw6VJtZklTm6uILMOL1ETOTlbA02dhF16fjcXlAACirDp0Yo9pi91FrpEljOYkqAO9AX4WMbEjAPZrnaATfYmRqCTOlnrIK8xvgEPgIu/OOub7KBYh6AQn7U8QBNoASkXkn31dqyM2I+KosKy2VeJO9cjPTahhXtkW7zUFA6863Czt2oHqL6Rvwsjr+7TikfQ==",
      "body": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-gov-west-1\",\"eventTime\":\"2022-02-07T17:31:04.498Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROAWM7XM4I6Z3NL2ST2J:wphyo\"},\"requestParameters\":{\"sourceIPAddress\":\"128.149.246.219\"},\"responseElements\":{\"x-amz-request-id\":\"FM1CHA780PBP0YEY\",\"x-amz-id-2\":\"Vp12Q/ok1+Y/0WonTCoUjCCREZhJU3CO82uDbve6m6FqJsFGTMBcLdunqeMLmQ11ZECV6z2WFsak6EbjdIZTi/jL+crmwops\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"all-obj-create\",\"bucket\":{\"name\":\"lsmd-data-bucket\",\"ownerIdentity\":{\"principalId\":\"440216117821\"},\"arn\":\"arn:aws-us-gov:s3:::lsmd-data-bucket\"},\"object\":{\"key\":\"manual_test/zipped_upload/jpl.calendar.2022.png\",\"size\":841141,\"eTag\":\"1477b70ad2cd03be3d72a49dc58fb52a\",\"sequencer\":\"0062015756ACFAA1FD\"}}}]}",
      "attributes": {
        "ApproximateReceiveCount": "6",
        "SentTimestamp": "1644255065441",
        "SenderId": "AIDALVP5ID7KAVBU2CQ3O",
        "ApproximateFirstReceiveTimestamp": "1644255065441"
      },
      "messageAttributes": {},
      "md5OfBody": "00cb0a5ed122862537ab6115dae36f69",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws-us-gov:sqs:us-gov-west-1:440216117821:send_records_to_es",
      "awsRegion": "us-gov-west-1"
    }
  ]
}
#######
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-gov-west-1",
      "eventTime": "2022-02-07T17:31:04.498Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AROAWM7XM4I6Z3NL2ST2J:wphyo"
      },
      "requestParameters": {
        "sourceIPAddress": "128.149.246.219"
      },
      "responseElements": {
        "x-amz-request-id": "FM1CHA780PBP0YEY",
        "x-amz-id-2": "Vp12Q/ok1+Y/0WonTCoUjCCREZhJU3CO82uDbve6m6FqJsFGTMBcLdunqeMLmQ11ZECV6z2WFsak6EbjdIZTi/jL+crmwops"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "all-obj-create",
        "bucket": {
          "name": "lsmd-data-bucket",
          "ownerIdentity": {
            "principalId": "440216117821"
          },
          "arn": "arn:aws-us-gov:s3:::lsmd-data-bucket"
        },
        "object": {
          "key": "manual_test/zipped_upload/jpl.calendar.2022.png",
          "size": 841141,
          "eTag": "1477b70ad2cd03be3d72a49dc58fb52a",
          "sequencer": "0062015756ACFAA1FD"
        }
      }
    }
  ]
}
"""
import json
from urllib.parse import unquote_plus

from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator
from parquet_flask.cdms_lambda_func.s3_records.s3_event_validator_abstract import S3EventValidatorAbstract
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = LambdaLoggerGenerator.get_logger(__name__, log_level=LambdaLoggerGenerator.get_level_from_env())


class S3ToSqs(S3EventValidatorAbstract):
    OUTER_SCHEMA = {
        'type': 'object',
        'properties': {
            'Records': {
                'type': 'array',
                'minItems': 1,
                'maxItems': 100,
                'items': {
                    'type': 'object',
                    'properties': {
                        'body': {'type': 'string', 'minLength': 1}
                    },
                    'required': ['body']
                }
            }
        },
        'required': ['Records']
    }
    S3_RECORD_SCHEMA = {
        'type': 'object',
        'properties': {'Records': {
            'type': 'array',
            'minItems': 1,
            'maxItems': 1,
            'items': {
                'type': 'object',
                'properties': {
                    'eventName': {'type': 'string'},
                    's3': {
                        'type': 'object',
                        'properties': {
                            'bucket': {
                                'type': 'object',
                                'properties': {'name': {'type': 'string', 'minLength': 1}},
                                'required': ['name']
                            },
                            'object': {
                                'type': 'object',
                                'properties': {'key': {'type': 'string', 'minLength': 1}},
                                'required': ['key']
                            }},
                        'required': ['bucket', 'object']
                    }
                },
                'required': ['eventName', 's3']
            }
        }},
        'required': ['Records']
    }

    def __init__(self, event) -> None:
        super().__init__(event)
        self.__event = event
        self.__s3_record = None
        self.__is_valid()

    def __is_valid(self):
        is_valid, validation_err = GeneralUtils.is_json_valid(self.__event, self.OUTER_SCHEMA)
        if is_valid is False:
            raise ValueError(f'invalid OUTER_SCHEMA: {self.__event} vs {self.OUTER_SCHEMA}. errors: {validation_err}')
        self.__s3_record = []

        # Unpack SQS messages from lambda event

        for each_sqs_record in self.__event['Records']:
            s3_record = each_sqs_record['body']
            if isinstance(s3_record, str):
                s3_record = json.loads(s3_record)
            is_valid, validation_err = GeneralUtils.is_json_valid(s3_record, self.S3_RECORD_SCHEMA)
            if is_valid is False:
                raise ValueError(f'invalid S3_RECORD_SCHEMA: {s3_record} vs {self.S3_RECORD_SCHEMA}. errors: {validation_err}')

            # Unpack S3 events from SQS message. (I believe multiple events can be consolidated into single messages)

            for each_s3_record in s3_record['Records']:
                self.__s3_record.append(each_s3_record)
        return self

    def size(self):
        if self.__s3_record is None:
            self.__is_valid()
        return len(self.__s3_record)

    def get_s3_url(self, index: int):
        if self.__s3_record is None:
            self.__is_valid()
        if index >= len(self.__s3_record):
            raise ValueError(f'index: {index} is larger than s3_record array size: {len(self.__s3_record)}')
        s3_url = f"s3://{self.__s3_record[index]['s3']['bucket']['name']}/{self.__s3_record[index]['s3']['object']['key']}"
        LOGGER.debug(f'original s3_url: {s3_url}')
        s3_url = unquote_plus(s3_url)
        LOGGER.debug(f'unquoted s3_url: {s3_url}')
        return s3_url

    def get_event_name(self, index: int) -> str:
        if self.__s3_record is None:
            self.__is_valid()
        if index >= len(self.__s3_record):
            raise ValueError(f'index: {index} is larger than s3_record array size: {len(self.__s3_record)}')
        return self.__s3_record[index]['eventName']
