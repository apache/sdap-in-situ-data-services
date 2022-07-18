#!/usr/bin/env bash

apt-get update -y && apt-get install zip -y

ZIP_NAME='cdms_lambda_functions'
project_root_dir=${PWD}
software_version=`python3 ${project_root_dir}/setup.py --version`
zip_file="${project_root_dir}/${ZIP_NAME}__${software_version}.zip" ; # save the result file in current working directory

tmp_proj='/tmp/cdms_lambda_functions'

source_dir="/usr/local/lib/python3.8/site-packages/"

mkdir -p "$tmp_proj/cdms_lambda_functions" && \
cd $tmp_proj && \
cp -a "${project_root_dir}/parquet_flask/." "$tmp_proj/cdms_lambda_functions" && \
cp "${project_root_dir}/setup_lambda.py" $tmp_proj && \
python3 setup_lambda.py install && \
python3 setup_lambda.py install_lib && \
python -m pip uninstall boto3 -y && \
python -m pip uninstall botocore -y && \
cd ${source_dir}
rm -rf ${zip_file} && \
zip -r9 ${zip_file} . && \
echo "zipped to ${zip_file}"
