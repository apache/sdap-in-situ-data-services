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

from parquet_cli.audit_tool import audit
import os
import json
import logging

import boto3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)


# Build process:
# 1: cd to incubator-sdap-in-situ-data-services/parquet_flask/cdms_lambda_func/audit_tool
# 2: mkdir package
# 3: rename incubator-sdap-in-situ-data-services/setup_lambda.py to incubator-sdap-in-situ-data-services/setup.py
# 4: pip install --target ./package ../../..
# 5: cd package
# 6: zip -r ../audit_lambda.zip .
# 7: Upload zip to lambda
# 8: Clean up


def execute_code(event, context):
    state = None

    if event:
        if isinstance(event, str):
            event = json.loads(event)

        if 'State' in event:
            s3 = boto3.client('s3')
            s3.download_file(event['State']['Bucket'], event['State']['Key'], 'state.json')

            with open('state.json') as fp:
                state = json.load(fp)

            print('Loaded persisted state from S3', flush=True)

            s3.delete_object(Bucket=event['State']['Bucket'], Key=event['State']['Key'])

    print('INVOKING AUDIT TOOL', flush=True)
    print(event, flush=True)
    print(context, flush=True)
    print(state, flush=True)
    print('', flush=True)

    audit(
        'mock-s3',
        os.getenv('SQS_URL'),
        os.getenv('SNS_ARN'),
        state=state,
        lambda_ctx=context
    )
