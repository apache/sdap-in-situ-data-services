from parquet_flask.cdms_lambda_func.ingest_s3_to_cdms.ingest_s3_to_cdms import IngestS3ToCdms


def execute_code(event, context):
    IngestS3ToCdms().start(event)
    return
