import os

from tests.get_aws_creds import export_as_env

aws_creds = export_as_env()
for k, v in aws_creds.items():
    os.environ[k] = v
os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = ''
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''


"""
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN="""

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory

index = 'parquet_stats_v1'
es_url = 'https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com'
aws_es: ESAbstract = ESFactory().get_instance('AWS', index=index, base_url=es_url, port=443)
aws_es.index_one({'s3_url': 'test1'}, 'test21', index)
print(aws_es.query({'query': {'match_all': {}}}))
