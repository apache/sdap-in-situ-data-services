#!/usr/bin/env bash

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

ZIP_NAME='cdms_lambda_functions'
software_version=`python3 setup.py --version`
zip_file="${PWD}/${ZIP_NAME}__${software_version}.zip"
echo ${zip_file}
#aws --profile saml-pub s3 cp zip_file s3://cdms-dev-in-situ-parquet/cdms_lambda/
#aws --profile saml-pub lambda update-function-code --s3-key cdms_lambda/cdms_lambda_functions__0.1.1.zip --s3-bucket cdms-dev-in-situ-parquet --function-name arn:aws:lambda:us-west-2:848373852523:function:cdms-dev-in-situ-parquet-es-records --publish
aws --profile saml-pub lambda update-function-code --zip-file fileb://${zip_file} --function-name arn:aws:lambda:us-west-2:848373852523:function:cdms-dev-in-situ-parquet-es-records --publish
