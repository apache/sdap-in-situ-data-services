import json
import os
from time import sleep

from parquet_flask.aws.aws_ddb import AwsDdbProps, AwsDdb
from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.aws.queue_service_props import QueueServiceProps
from parquet_flask.aws.queue_sqs import QueueSQS

ncar_bucket = 'cdms-dev-ncar-in-situ-stage'
spurs_bucket = 'cdms-dev-spurs-in-situ-stage'
fsu_bucket = 'cdms-dev-fsu-in-situ-stage'

s3_bucket = spurs_bucket
s3_key = ''
ignoring_keys = {'argo-profiles-5903750.nc.json', 'argo-profiles-5903751.nc.json', 'D5904778.json', 'cdms_icoads_2015-07-01.json.gz'}

s3 = AwsS3()
sqs_props = QueueServiceProps()
sqs_props.queue_url = 'https://sqs.us-west-2.amazonaws.com/848373852523/cdms-dev-ingest-queue-1'
sqs = QueueSQS(sqs_props)
ddb_props = AwsDdbProps()
ddb_props.tbl_name = ''
ddb_props.hash_key = 's3_url'
ddb = AwsDdb(ddb_props)
all_files = [k for k, _ in s3.get_child_s3_files(s3_bucket, s3_key, lambda x:  x['Key'].endswith('.json'))]
# all_files = ['s3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-109456.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-109457.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-109458.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-109459.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-114632.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-114634.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-114638.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-29278.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-300234.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-30171.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-36482.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-36609.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73231.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73237.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73238.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73240.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73259.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73262.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73390.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73393.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73395.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-73400.json', 's3://cdms-dev-spurs-in-situ-stage/SPURS_Euro_drifter-92551.json']
# print(all_files)
# print("len(all_files): " + str(len(all_files)))
for each_key in all_files:
    # s3_url = f's3://{s3_bucket}/{each_key}'
    # if ddb.get_one_item(s3_url) is not None:
    #     print(f'{s3_url} exists. skipping')
    #     continue
    s3_msg_body = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-gov-west-1",
                "eventTime": "2023-04-13T11:30:04.498Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AROA4LBYFVFVRAI2X6TZX:Min.Liang.Kang@jpl.nasa.gov"
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
                        "name": s3_bucket,
                        "ownerIdentity": {
                            "principalId": "440216117821"
                        },
                        "arn": f"arn:aws-us-gov:s3:::{s3_bucket}"
                    },
                    "object": {
                        "key": each_key,
                        "size": 841141,
                        "eTag": "1477b70ad2cd03be3d72a49dc58fb52a",
                        "sequencer": "0062015756ACFAA1FD"
                    }
                }
            }
        ]
    }
    # print(json.dumps(s3_msg_body))
    # break
    sqs.send_msg(s3_msg_body)
    # NOTE uncomment it to send messages.
