import os

from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator


def execute_code(event, context):
    os.environ['master_spark_url'] = ''
    os.environ['spark_app_name'] = ''
    os.environ['parquet_file_name'] = ''
    os.environ['in_situ_schema'] = ''
    os.environ['authentication_type'] = ''
    os.environ['authentication_key'] = ''
    os.environ['parquet_metadata_tbl'] = ''
    LambdaLoggerGenerator.remove_default_handlers()

    from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_es_indexer import ParquetFileEsIndexer
    ParquetFileEsIndexer().start(event)
    return
