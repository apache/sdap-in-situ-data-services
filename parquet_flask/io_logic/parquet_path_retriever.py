from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.insitu.get_query_transformer import GetQueryTransformer
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath


class ParquetPathRetriever:
    def __init__(self, es_mw: ESAbstract, file_struct_setting: FileStructureSetting, base_path: str):
        self.__base_path = base_path
        self.__file_struct_setting = file_struct_setting
        self.__es_mw = es_mw

    def start(self, query_dict) -> [PartitionedParquetPath]:
        query_transformer = GetQueryTransformer(self.__file_struct_setting)
        query_object = query_transformer.transform_param(query_dict)
        es_terms = query_transformer.generate_dsl_conditions(query_object)
        es_dsl = {
            'query': {
                'bool': {
                    'must': es_terms
                }
            },
            'sort': [
                {k: {'order': 'asc'}} for k in self.__file_struct_setting.get_es_index_schema_parquet_stats()['mappings']['properties'].keys()
            ]
        }
        result = self.__es_mw.query_pages(es_dsl)
        result = [PartitionedParquetPath(self.__base_path, self.__file_struct_setting.get_partitioning_columns()).load_from_es(k['_source']) for k in result['items']]
        return result
