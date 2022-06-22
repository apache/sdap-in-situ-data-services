import argparse
import base64
import logging
import os

os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = ''
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''

from parquet_flask.cdms_lambda_func.lambda_func_env import LambdaFuncEnv


class IngestS3Entry:
    BUCKET_NAME_KEY = 'BUCKET_NAME'
    KEY_PREFIX_KEY = 'KEY_PREFIX'

    def __init__(self):
        self.__a = ''

    def __get_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Ingesting 1 or more S3 files into Parquet. Note that AWS environment variables should be set before running this")
        parser.add_argument(f'--{LambdaFuncEnv.CDMS_DOMAIN}',
                            help="CDMS Flask domain where ingestion endpoint resides. Need to include `/insitu` prefix",
                            metavar="http://localhost:9801/insitu",
                            required=True)
        parser.add_argument(f'--{LambdaFuncEnv.CDMS_BEARER_TOKEN}',
                            help="plain-text security token that is set in CDMS Flask pod during K8s deployment. Check in Dockerfile",
                            metavar="mock-token",
                            required=True)
        parser.add_argument(f'--{LambdaFuncEnv.PARQUET_META_TBL_NAME}',
                            help="dynamo DB table where parquet file ingestion records are stored. Check in Values.yaml",
                            metavar="cdms_parquet_meta_dev_v1",
                            required=True)
        parser.add_argument(f'--{self.BUCKET_NAME_KEY}',
                            help="name of S3 bucket",
                            metavar="icoads-bucket",
                            required=True)
        parser.add_argument(f'--{self.KEY_PREFIX_KEY}',
                            help="s3 prefix. It will ingest all files starting with this prefix. If all filees need to be ingested, pass empty value. If only 1 file needs to be ingested, pass the exact file path",
                            metavar='2021/01/01/samplefile.json.gz',
                            required=True)
        parser.add_argument(f'--{LambdaFuncEnv.LOG_LEVEL}',
                            help="python log level in integer.",
                            default='10',
                            metavar='10',
                            required=False)
        return parser.parse_args()

    def start(self):
        options = self.__get_args()
        logging.basicConfig(level=int(getattr(options, LambdaFuncEnv.LOG_LEVEL)),
                            format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")


        os.environ[LambdaFuncEnv.LOG_LEVEL] = getattr(options, LambdaFuncEnv.LOG_LEVEL)
        os.environ[LambdaFuncEnv.CDMS_DOMAIN] = getattr(options, LambdaFuncEnv.CDMS_DOMAIN)
        os.environ[LambdaFuncEnv.CDMS_BEARER_TOKEN] = base64.standard_b64encode(getattr(options, LambdaFuncEnv.CDMS_BEARER_TOKEN).encode()).decode()
        os.environ[LambdaFuncEnv.PARQUET_META_TBL_NAME] = getattr(options, LambdaFuncEnv.PARQUET_META_TBL_NAME)
        bucket_name = getattr(options, self.BUCKET_NAME_KEY)
        key_prefix = getattr(options, self.KEY_PREFIX_KEY)

        from parquet_flask.cdms_lambda_func.ingest_s3_to_cdms.ingest_s3_to_cdms import IngestS3ToCdms
        from parquet_flask.aws.aws_s3 import AwsS3

        s3 = AwsS3()
        for key, size in s3.get_child_s3_files(bucket_name, key_prefix,
                                               lambda x: x['Key'].endswith('.json') or x['Key'].endswith('.json.gz')):
            try:
                print(f'working on: {key}')
                IngestS3ToCdms().start(event={'s3_url': f's3://{bucket_name}/{key}'})
            except Exception as e:
                print(f'error while processing: s3://{bucket_name}/{key}. details: {str(e)}')
        return


if __name__ == '__main__':
    """
    Sample usage: 
    
    python3 -m parquet_cli.ingest_s3 \
      --LOG_LEVEL 30 \
      --CDMS_DOMAIN https://doms.jpl.nasa.gov/insitu  \
      --CDMS_BEARER_TOKEN Mock-Token  \
      --PARQUET_META_TBL_NAME cdms_parquet_meta_dev_v1  \
      --BUCKET_NAME cdms-dev-ncar-in-situ-stage  \
      --KEY_PREFIX cdms_icoads_2017-01-01.json
  
  
    """
    IngestS3Entry().start()
