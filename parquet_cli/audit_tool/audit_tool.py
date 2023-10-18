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

def audit(format='plain', output_file=None):
    if format == 'mock-s3' and not output_file:
        raise ValueError('Output file MUST be defined with mock-s3 output format')

    # Check if AWS credentials are set
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
    if AWS_ACCESS_KEY_ID is None or AWS_SECRET_ACCESS_KEY is None:
        print('AWS credentials are not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.')
        exit(1)

    # Check if OpenSearch parameters are set
    OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT', None)
    OPENSEARCH_PORT = os.environ.get('OPENSEARCH_PORT', 443)
    OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', None)
    OPENSEARCH_PATH_PREFIX = append_slash(os.environ.get('OPENSEARCH_PATH_PREFIX', None))
    OPENSEARCH_BUCKET = os.environ.get('OPENSEARCH_BUCKET', None)
    OPENSEARCH_ID_PREFIX = append_slash(os.environ.get('OPENSEARCH_ID_PREFIX', f's3://{OPENSEARCH_BUCKET}/'))
    if OPENSEARCH_ENDPOINT is None or OPENSEARCH_PORT is None or OPENSEARCH_ID_PREFIX is None or OPENSEARCH_INDEX is None or OPENSEARCH_PATH_PREFIX is None or OPENSEARCH_BUCKET is None:
        print('OpenSearch parameters are not set. Please set OPENSEARCH_ENDPOINT, OPENSEARCH_PORT, OPENSEARCH_ID_PREFIX, OPENSEARCH_INDEX, OPENSEARCH_PATH_PREFIX, OPENSEARCH_BUCKET.')
        exit(1)

    # AWS session
    aws_session_param = {}
    aws_session_param['aws_access_key_id'] = AWS_ACCESS_KEY_ID
    aws_session_param['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY
    aws_session = boto3.Session(**aws_session_param)

    # AWS auth
    aws_auth = AWS4Auth(aws_session.get_credentials().access_key, aws_session.get_credentials().secret_key, aws_session.region_name, 'es')

    # S3 paginator
    s3 = aws_session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    opensearch_client = Elasticsearch(
        hosts=[{'host': OPENSEARCH_ENDPOINT, 'port': OPENSEARCH_PORT}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Go through all files in a bucket
    response_iterator = paginator.paginate(Bucket=OPENSEARCH_BUCKET, Prefix=OPENSEARCH_PATH_PREFIX)
    count = 0
    error_count = 0
    error_s3_keys = []
    missing_keys = []
    print('processing... will print out S3 keys that cannot find a match...', flush=True)
    for page in response_iterator:
        for obj in page.get('Contents', []):
            count += 1
            try:
                # Search key in opensearch
                opensearch_id = os.path.join(OPENSEARCH_ID_PREFIX + obj['Key'])
                opensearch_response = opensearch_client.get(index=OPENSEARCH_INDEX, id=opensearch_id)
                if opensearch_response is None or not type(opensearch_response) is dict or not opensearch_response['found']:
                    error_count += 1
                    error_s3_keys.append(obj['Key'])
                    sys.stdout.write("\x1b[2k")
                    print(obj['Key'], flush=True)
                    missing_keys.append(obj['Key'])
            except NotFoundError as e:
                error_count += 1
                error_s3_keys.append(obj['Key'])
                sys.stdout.write("\x1b[2k")
                print(obj['Key'], flush=True)
                missing_keys.append(obj['Key'])
            except Exception as e:
                error_count += 1

            print(f'processed {count} files', end='\r', flush=True)
    print('')

    print(f'Found {len(missing_keys):,} missing keys')

    if format == 'plain':
        if output_file:
            with open(output_file, 'w') as f:
                for key in missing_keys:
                    f.write(key + '\n')
        else:
            print('Not writing to file as none was given')
    else:
        pass
        # TODO: Format to mock SQS messages. One key per message cause the lambda only expects one object per message


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
             '\'mock-s3\' will output missing keys to SQS (-o is required as the SQS queue URL), formatted as an S3 object '
             'created event'
    )

    args = parser.parse_args()
    audit(args.format, args.output)
