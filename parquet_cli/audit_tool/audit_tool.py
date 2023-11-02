########################################################################################################################
# Description: This script is used to audit the parquet files in S3 against records OpenSearch. It will print out the  #
#              S3 keys of the files that are not found in OpenSearch.                                                  #
#                                                                                                                      #
# Usage:                                                                                                               #
#   python audit_tool.py [ -o <output_filename> ]                                                                      #
#                                                                                                                      #
# Environment variables:                                                                                               #
#   AWS_ACCESS_KEY_ID: AWS access key ID                                                                               #
#   AWS_SECRET_ACCESS_KEY: AWS secret access key                                                                       #
#   OPENSEARCH_ENDPOINT: OpenSearch endpoint                                                                           #
#   OPENSEARCH_PORT: OpenSearch port                                                                                   #
#   OPENSEARCH_ID_PREFIX: OpenSearch ID prefix e.g. s3://cdms-dev-in-situ-parquet                                      #
#   OPENSEARCH_INDEX: OpenSearch index e.g. [ parquet_stats_alias | entry_file_records_alias ]                         #
#   OPENSEARCH_PATH_PREFIX: OpenSearch path prefix (use '' if no prefix needed)                                        #
#   OPENSEARCH_BUCKET: OpenSearch bucket                                                                               #
########################################################################################################################


import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection, NotFoundError
import os
import sys
import argparse
import textwrap
import json
import logging
from tempfile import NamedTemporaryFile
from datetime import timedelta, datetime, timezone

from parquet_flask.aws import AwsSQS, AwsSNS
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator


# logging.basicConfig(
#     level=logging.INFO,
#     format=
# )

LambdaLoggerGenerator.remove_default_handlers()
logger = LambdaLoggerGenerator.get_logger(
    __name__,
    log_format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
LambdaLoggerGenerator.get_logger('elasticsearch', log_level=logging.WARNING)


PHASES = dict(
    start=0,
    list=1,
    audit=2
)


# Append a slash to the end of a string if it doesn't already have one
def append_slash(string: str):
    if string is None:
        return None
    elif string == '':
        return string
    elif string[-1] != '/':
        return string + '/'
    else:
        return string


def key_to_sqs_msg(key: str, bucket: str):
    s3_event = {
        'Records': [
            {
                'eventName': 'ObjectCreated:Put',
                's3': {
                    'bucket': {'name': bucket},
                    'object': {'key': key}
                }
            }
        ]
    }

    sqs_body = json.dumps(s3_event)

    return sqs_body


def reinvoke(state, bucket, s3_client, lambda_client, function_name):
    state['lastListTime'] = state['lastListTime'].strftime("%Y-%m-%dT%H:%M:%S%z")

    logger.info('Preparing to reinvoke. Persisting audit state to S3')

    object_data = json.dumps(state).encode('utf-8')

    s3_client.put_object(Bucket=bucket, Key='AUDIT_STATE.json', Body=object_data)

    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',
        Payload=json.dumps({"State": {"Bucket": bucket, "Key": "AUDIT_STATE.json"}})
    )

    logger.info(f'Lambda response: {repr(response)}')


def audit(format='plain', output_file=None, sns_topic=None, state=None, lambda_ctx=None):
    if format == 'mock-s3' and not output_file:
        raise ValueError('Output file MUST be defined with mock-s3 output format')

    if state is None:
        state = {}

    # Check if AWS credentials are set
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
    AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', None)
    if AWS_ACCESS_KEY_ID is None or AWS_SECRET_ACCESS_KEY is None:
        logger.error('AWS credentials are not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.')
        exit(1)

    # Check if OpenSearch parameters are set
    OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT', None)
    OPENSEARCH_PORT = os.environ.get('OPENSEARCH_PORT', 443)
    OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', None)
    OPENSEARCH_PATH_PREFIX = append_slash(os.environ.get('OPENSEARCH_PATH_PREFIX', None))
    OPENSEARCH_BUCKET = os.environ.get('OPENSEARCH_BUCKET', None)
    OPENSEARCH_ID_PREFIX = append_slash(os.environ.get('OPENSEARCH_ID_PREFIX', f's3://{OPENSEARCH_BUCKET}/'))
    if OPENSEARCH_ENDPOINT is None or OPENSEARCH_PORT is None or OPENSEARCH_ID_PREFIX is None or OPENSEARCH_INDEX is None or OPENSEARCH_PATH_PREFIX is None or OPENSEARCH_BUCKET is None:
        logger.error('OpenSearch parameters are not set. Please set OPENSEARCH_ENDPOINT, OPENSEARCH_PORT, OPENSEARCH_ID_PREFIX, OPENSEARCH_INDEX, OPENSEARCH_PATH_PREFIX, OPENSEARCH_BUCKET.')
        exit(1)

    # AWS session
    aws_session_param = {}
    aws_session_param['aws_access_key_id'] = AWS_ACCESS_KEY_ID
    aws_session_param['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY

    if AWS_SESSION_TOKEN:
        aws_session_param['aws_session_token'] = AWS_SESSION_TOKEN

    aws_session = boto3.Session(**aws_session_param)

    # AWS auth
    aws_auth = AWS4Auth(
        aws_session.get_credentials().access_key,
        aws_session.get_credentials().secret_key,
        aws_session.region_name,
        'es',
        session_token=aws_session.get_credentials().token
    )

    # S3 paginator
    s3 = aws_session.client('s3')
    lambda_client = aws_session.client('lambda')

    phase = state.get('state', 'start')
    phase = PHASES[phase]
    marker = state.get('marker')
    keys: list = state.get('keys', [])

    opensearch_client = Elasticsearch(
        hosts=[{'host': OPENSEARCH_ENDPOINT, 'port': OPENSEARCH_PORT}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Go through all files in a bucket
    count = 0
    error_count = 0
    error_s3_keys = []
    missing_keys = []
    logger.info('processing... will print out S3 keys that cannot find a match...')
    if phase < 2:
        if phase == 0:
            logger.info(f'Starting listing of bucket {OPENSEARCH_BUCKET}')
            state['listStartTime'] = datetime.now(timezone.utc)
        else:
            logger.info(f'Resuming listing of bucket {OPENSEARCH_BUCKET}')
        while True:
            list_kwargs = dict(
                Bucket=OPENSEARCH_BUCKET,
                Prefix=OPENSEARCH_PATH_PREFIX,
                MaxKeys=1000
            )

            if marker:
                list_kwargs['ContinuationToken'] = marker

            page = s3.list_objects_v2(**list_kwargs)
            keys_to_add = []

            for key in page.get('Contents', []):
                if key['Key'].endswith('parquet'):
                    keys_to_add.append(key['Key'])

            keys.extend(keys_to_add)

            logger.info(f"Listed page of {len(page.get('Contents', [])):,} objects; selected {len(keys_to_add):,}; "
                        f"total={len(keys):,}")

            if lambda_ctx:
                remaining_time = timedelta(milliseconds=lambda_ctx.get_remaining_time_in_millis())
                logger.info(f'Remaining time: {remaining_time}')

            if not page['IsTruncated']:
                break
            else:
                marker = page['NextContinuationToken']

            if lambda_ctx is not None and lambda_ctx.get_remaining_time_in_millis() < (60 * 1000):
                logger.warning('Lambda is about to time out, re-invoking to resume from this key')

                state = dict(
                    state='list',
                    marker=marker,
                    keys=keys,
                    listStartTime=state['lastListTime']
                )

                reinvoke(state, OPENSEARCH_BUCKET, s3, lambda_client, lambda_ctx.function_name)
                return

        state['lastListTime'] = state['listStartTime']
        del state['listStartTime']

    # if phase == 3 and marker is not None and marker in keys:
    #     logger.info(f'Resuming audit from key {marker}')
    #     index = keys.index(marker)
    # else:
    #     logger.info('Starting audit from the beginning')
    #     index = 0
    #
    # keys = keys[index:]

    n_keys = len(keys)

    logger.info(f'Beginning audit on {n_keys:,} keys...')

    need_to_resume = False

    while len(keys) > 0:
        key = keys.pop(0)
        count += 1
        try:
            # Search key in opensearch
            opensearch_id = os.path.join(OPENSEARCH_ID_PREFIX + key)
            opensearch_response = opensearch_client.get(index=OPENSEARCH_INDEX, id=opensearch_id)
            if opensearch_response is None or not type(opensearch_response) is dict or not opensearch_response['found']:
                error_count += 1
                error_s3_keys.append(key)
                sys.stdout.write("\x1b[2k")
                logger.info(key)
                missing_keys.append(key)
        except NotFoundError as e:
            error_count += 1
            error_s3_keys.append(key)
            sys.stdout.write("\x1b[2k")
            logger.info(key)
            missing_keys.append(key)
        except Exception as e:
            error_count += 1

        if count % 50 == 0:
            logger.info(f'Checked {count} files [{(count/n_keys)*100:7.3f}%]')
            if lambda_ctx:
                remaining_time = timedelta(milliseconds=lambda_ctx.get_remaining_time_in_millis())
                logger.info(f'Remaining time: {remaining_time}')

        if lambda_ctx is not None and lambda_ctx.get_remaining_time_in_millis() < (60 * 1000):
            logger.warning('Lambda is about to time out, re-invoking to resume from this key')

            state = dict(
                state='audit',
                marker=key,
                keys=keys,
                lastListTime=state['lastListTime']
            )

            need_to_resume = True

            break

    logger.info(f'Checked {count} files')
    logger.info(f'Found {len(missing_keys):,} missing keys')

    if len(missing_keys) > 0:
        if format == 'plain':
            if output_file:
                with open(output_file, 'w') as f:
                    for key in missing_keys:
                        f.write(key + '\n')
            else:
                logger.info('Not writing to file as none was given')
        else:
            sqs_messages = [key_to_sqs_msg(k, OPENSEARCH_BUCKET) for k in missing_keys]

            sqs = AwsSQS()

            sqs_response = None

            for m in sqs_messages:
                sqs_response = sqs.send_message(output_file, m)

            logger.info(f'SQS response: {repr(sqs_response)}')

            if sns_topic:
                sns = AwsSNS()

                sns_response = sns.publish(
                    sns_topic,
                    f'Parquet stats audit found {len(missing_keys):,} missing keys. Trying to republish to SQS.',
                    'Insitu audit'
                )

                logger.info(f'SNS response: {repr(sns_response)}')

    if need_to_resume:
        reinvoke(state, OPENSEARCH_BUCKET, s3, lambda_client, lambda_ctx.function_name)
        return

    # Finished, reset state to just last list time

    logger.info('Audit complete! Persisting state to S3')

    state = {'lastListTime': state['lastListTime'].strftime("%Y-%m-%dT%H:%M:%S%z")}

    object_data = json.dumps(state).encode('utf-8')

    s3.put_object(Bucket=OPENSEARCH_BUCKET, Key='AUDIT_STATE.json', Body=object_data)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Audit parquet files in S3 against records in OpenSearch',
        epilog=textwrap.dedent('''\
            Environment variables:  (describe what they are for & provide examples where appropriate
                AWS_ACCESS_KEY_ID : AWS access key ID for S3 bucket & OpenSearch index
                AWS_SECRET_ACCESS_KEY : AWS secret access key for S3 bucket & OpenSearch index
                AWS_REGION : AWS region for S3 bucket & OpenSearch index
                OPENSEARCH_ENDPOINT : Endpoint for OpenSearch domain
                OPENSEARCH_PORT : Port to connect to OpenSearch (Default: 443)
                OPENSEARCH_BUCKET : Name of the bucket storing ingested Parquet files.
                OPENSEARCH_PATH_PREFIX : Key prefix for objects in OPENSEARCH_BUCKET to audit.
                OPENSEARCH_ID_PREFIX : S3 URI prefix for the id field in OpenSearch documents. Defaults to 's3://<OPENSEARCH_BUCKET>/'
                OPENSEARCH_INDEX : OpenSearch index to audit
        '''),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '-o', '--output-file',
        nargs='?',
        type=str,
        help='file to output the S3 keys of the files that are not found in OpenSearch',
        dest='output'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['plain', 'mock-s3'],
        default='plain',
        dest='format',
        help='Output format. \'plain\' will output keys of missing parquet files to the output file in plain text. '
             '\'mock-s3\' will output missing keys to SQS (-o is required as the SQS queue URL), formatted as an S3 '
             'object created event'
    )

    args = parser.parse_args()
    audit(args.format, args.output)
