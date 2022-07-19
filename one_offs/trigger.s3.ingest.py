import os

from flask import Flask

from parquet_flask.cdms_lambda_func.lambda_func_env import LambdaFuncEnv

if __name__ == '__main__':
    # app = Flask('my_app')

    # os.environ['master_spark_url'] = ''
    # os.environ['spark_app_name'] = ''
    # os.environ['parquet_file_name'] = ''
    # os.environ['aws_access_key_id'] = ''
    # os.environ['aws_secret_access_key'] = ''
    # os.environ['aws_session_token'] = ''
    # os.environ['in_situ_schema'] = ''
    # os.environ['authentication_type'] = ''
    # os.environ['authentication_key'] = ''
    # os.environ['parquet_metadata_tbl'] = ''

    os.environ[LambdaFuncEnv.LOG_LEVEL] = '20'
    os.environ[LambdaFuncEnv.CDMS_DOMAIN] = 'http://localhost:9801/insitu'
    os.environ[LambdaFuncEnv.CDMS_BEARER_TOKEN] = 'aaa'
    os.environ[LambdaFuncEnv.PARQUET_META_TBL_NAME] = 'cdms_parquet_meta_dev_v1'
    from parquet_flask.cdms_lambda_func.ingest_s3_to_cdms.ingest_s3_to_cdms import IngestS3ToCdms
    from parquet_flask.aws.aws_s3 import AwsS3

    # local_file_path = s3.set_s3_url('s3://cdms-dev-fsu-in-situ-stage/KAOU_20170222.json.gz').download('/tmp')

    # bucket_name = 'nasa-cdms.saildrone.com'
    # key_prefix = '1021_atlantic'
    # bucket_name = 'cdms-dev-fsu-in-situ-stage'
    # key_prefix = ''
    bucket_name = 'cdms-dev-ncar-in-situ-stage'
    key_prefix = ''
    s3 = AwsS3()
    for key, size in s3.get_child_s3_files(bucket_name, key_prefix, lambda x: x['Key'].endswith('.json') or x['Key'].endswith('.json.gz')):
        try:
            print(f'working on: {key}')
            # print(s3.set_s3_url(f's3://cdms-dev-fsu-in-situ-stage/{key}').get_s3_obj_size())
            print(IngestS3ToCdms().start(event={'s3_url': f's3://{bucket_name}/{key}'}))
        except Exception as e:
            print(e)
