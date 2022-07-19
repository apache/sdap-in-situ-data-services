from parquet_flask.cdms_lambda_func.index_to_es.parquet_file_es_indexer import ParquetFileEsIndexer


def execute_code(event, context):
    ParquetFileEsIndexer().start(event)
    return
