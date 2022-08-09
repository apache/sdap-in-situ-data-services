import json
import os

from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants

os.environ[CdmsLambdaConstants.es_url] = 'https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com/'
os.environ[CdmsLambdaConstants.es_index] = 'parquet_stats_v1'
os.environ[CdmsLambdaConstants.cdms_url] = 'http://localhost:30801/insitu/1.0/extract_stats/'
os.environ[CdmsLambdaConstants.cdms_url] = 'https://doms.jpl.nasa.gov/insitu/1.0/extract_stats/'
os.environ[CdmsLambdaConstants.parquet_base_folder] = 'CDMS_insitu.geo3.parquet'
os.environ[CdmsLambdaConstants.insitu_schema_file] = '/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json'

from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_es_indexer import ParquetFileEsIndexer


s3_record_event = {
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
                    "name": "cdms-dev-in-situ-parquet",
                    "ownerIdentity": {
                        "principalId": "440216117821"
                    },
                    "arn": "arn:aws-us-gov:s3:::lsmd-data-bucket"
                },
                "object": {
                    "key": "CDMS_insitu.geo3.parquet/provider=NCAR/project=ICOADS Release 3.0/platform_code=17/geo_spatial_interval=10_140/year=2017/month=7/job_id=3476f5b7-4441-4c82-93fd-161658ba8657/part-00000-74ebb882-3536-435b-b736-96bf3be9ee29.c000.gz.parquet",
                    "size": 841141,
                    "eTag": "1477b70ad2cd03be3d72a49dc58fb52a",
                    "sequencer": "0062015756ACFAA1FD"
                }
            }
        }
    ]
}
sample_event = {
    "Records": [
        {
            "messageId": "6210f778-d081-4ae9-a861-8534d612dfae",
            "receiptHandle": "AQEBk55DchogyQzpVsH1A4YEj4K/PcVuIG9Em/a6/4AHIA4G5vLPiHVElNiuMfYc1ussk2U//JwZbD788Fv8u6W22L3AJ1U8EIcGJ57aibpmd6tSCWLS5q5FA4u2X2Jq5z+lCX5NZXzNDYMqMJaCGtBkcYi4a9LDXtD+U7HWX0V8OPhFFF2a1qUu+E05c16f5OmE7wRJ3SFrRmtJOhp2DigKKsw6VJtZklTm6uILMOL1ETOTlbA02dhF16fjcXlAACirDp0Yo9pi91FrpEljOYkqAO9AX4WMbEjAPZrnaATfYmRqCTOlnrIK8xvgEPgIu/OOub7KBYh6AQn7U8QBNoASkXkn31dqyM2I+KosKy2VeJO9cjPTahhXtkW7zUFA6863Czt2oHqL6Rvwsjr+7TikfQ==",
            "body": json.dumps(s3_record_event),
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
es_indexer = ParquetFileEsIndexer()
es_indexer.start(sample_event)
