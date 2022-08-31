#!/usr/bin/env bash
ZIP_NAME='cdms_lambda_functions'
software_version=`python3 setup.py --version`
zip_file="${PWD}/${ZIP_NAME}__${software_version}.zip"
echo ${zip_file}
#aws --profile saml-pub s3 cp zip_file s3://cdms-dev-in-situ-parquet/cdms_lambda/
#aws --profile saml-pub lambda update-function-code --s3-key cdms_lambda/cdms_lambda_functions__0.1.1.zip --s3-bucket cdms-dev-in-situ-parquet --function-name arn:aws:lambda:us-west-2:848373852523:function:cdms-dev-in-situ-parquet-es-records --publish
aws --profile saml-pub lambda update-function-code --zip-file fileb://${zip_file} --function-name arn:aws:lambda:us-west-2:848373852523:function:cdms-dev-in-situ-parquet-es-records --publish
