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

apt-get update -y && apt-get install zip -y

ZIP_NAME='cdms_lambda_functions'
project_root_dir=${PWD}
software_version=`python3 ${project_root_dir}/setup.py --version`
zip_file="${project_root_dir}/${ZIP_NAME}__${software_version}.zip" ; # save the result file in current working directory

tmp_proj='/tmp/parquet_flask'

source_dir="/usr/local/lib/python3.7/site-packages/"

mkdir -p "/etc" && \
cp "${project_root_dir}/in_situ_schema.json" /etc/ && \
mkdir -p "$tmp_proj/parquet_flask" && \
cd $tmp_proj && \
cp -a "${project_root_dir}/parquet_flask/." "$tmp_proj/parquet_flask" && \
cp "${project_root_dir}/setup_lambda.py" $tmp_proj && \
python3 setup_lambda.py install && \
python3 setup_lambda.py install_lib && \
python -m pip uninstall boto3 -y && \
python -m pip uninstall botocore -y && \
cd ${source_dir}
rm -rf ${zip_file} && \
zip -r9 ${zip_file} . && \
echo "zipped to ${zip_file}"
