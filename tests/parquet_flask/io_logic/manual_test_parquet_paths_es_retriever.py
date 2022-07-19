import os

os.environ['master_spark_url'] = ''
os.environ['spark_app_name'] = ''
os.environ['parquet_file_name'] = ''
os.environ['in_situ_schema'] = ''
os.environ['authentication_type'] = ''
os.environ['authentication_key'] = ''
os.environ['parquet_metadata_tbl'] = ''
os.environ['es_url'] = ''

from tests.get_aws_creds import export_as_env

aws_creds = export_as_env()
for k, v in aws_creds.items():
    os.environ[k] = v

from parquet_flask.io_logic.parquet_paths_es_retriever import ParquetPathsEsRetriever
from parquet_flask.io_logic.query_v2 import QueryProps

props = QueryProps()
props.provider = 'Florida State University, COAPS'
props.project = 'SAMOS'
props.platform_code = ['30', '31', '32']

props.min_datetime = '2017-01-25T09:00:00Z'
props.max_datetime = '2018-10-24T09:00:00Z'
props.min_lat_lon = (-44, 100.0)
props.max_lat_lon = (-25.0, 132.38330739034632)

#https://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination?startIndex=0&itemsPerPage=1000&startTime=&endTime=&bbox=&minDepth=0.0&maxDepth=5.0&provider=NCAR&project=ICOADS%20Release%203.0&platform=42
a = ParquetPathsEsRetriever('', props).load_es_from_config('https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com/', 'parquet_stats_v1', 443)
print([k.generate_path() for k in a.start()])
