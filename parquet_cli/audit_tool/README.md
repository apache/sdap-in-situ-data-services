# Insitu Stats Audit and Repair

A bug has been observed where the Parquet files produced at ingestion are missed by the stats extraction. This is 
problematic because, in addition to the stats endpoint being incorrect, the spatiotemporal information in stats records
is used in subsetting, leading to the appearance of missing data as only the Parquet files with extant stats records are 
processed at subset time.

Until the cause is identified & fixed, this tool can be used to verify that all the Parquet files in the destination S3
bucket are present in the stats index. It can either list the missing keys to console with the added option of listing 
them to a file as well, or it can publish them to AWS SQS as mock-S3 ObjectCreated events to re-trigger the stats 
extraction Lambda with the missing key. Optionally, it can notify users to missing keys via publishing to an AWS SNS 
topic.

There are two ways it's designed to be run:

## 1: Locally

1. Clone the repo: `git clone --branch data-validation-tool-lambda https://github.com/RKuttruff/incubator-sdap-in-situ-data-services.git`
2. `cd incubator-sdap-in-situ-data-services`
3. Run `python setup_lambda.py install` 
4. cd `parquet_cli/audit_tool`
5. Usage: 
```
(parquet_flask) rileykk@MT-407178 audit_tool % python audit_tool.py -h
usage: audit_tool.py [-h] [-o [OUTPUT]] [-f {plain,mock-s3}]

Audit parquet files in S3 against records in OpenSearch

optional arguments:
  -h, --help            show this help message and exit
  -o [OUTPUT], --output-file [OUTPUT]
                        file to output the S3 keys of the files that are not found in OpenSearch
  -f {plain,mock-s3}, --format {plain,mock-s3}
                        Output format. 'plain' will output keys of missing parquet files to the output file in plain text. 'mock-s3' will output missing keys to SQS (-o is required as the SQS queue URL), formatted as an S3 object created event

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
```
Example usage:
```
export AWS_ACCESS_KEY_ID=<secret>  
export AWS_REGION=us-west-2  
export AWS_SECRET_ACCESS_KEY=<secret>  
export OPENSEARCH_BUCKET=insitu-parquet-bucket  
export OPENSEARCH_ENDPOINT=<domain-endpoint>.us-west-2.es.amazonaws.com  
export OPENSEARCH_ID_PREFIX=s3://insitu-parquet-bucket/  
export OPENSEARCH_INDEX=parquet_stats_alias  
export OPENSEARCH_PATH_PREFIX=insitu.parquet/  
export OPENSEARCH_PORT=443

python audit.py -f mock-s3 -o https://sqs.us-west-2.amazonaws.com/<acct-id>/<queue-name>
```

## 2: Automated Lambda

Create a lambda function w/ python 3.8 runtime, x86_64 arch, with an execution role that can do the following:
- List, Get, Put, and Delete S3 target bucket
- Interact with ES/OpenSearch
- Invoke lambda functions
- SQS send message
- Optional: SNS publish

Config:
- Handler: `parquet_flask.cdms_lambda_func.audit_tool.execute_lambda.execute_code`
- Contact me for code .zip or follow the instructions in 
- Timeout: No less than 2 minutes, recommend at least 10 minutes
- Memory: 512 MB
- Ephemeral storage: 512 MB
- Environment variables: As with local invocation plus:
	- `SQS_URL`: URL of queue to publish to
	- `SNS_ARN`: ARN of SNS topic to publish to
- Concurrency: 
	- Use reserved: 1

Once configured/tested, you can use AWS EventBridge to trigger the function on a regular schedule.