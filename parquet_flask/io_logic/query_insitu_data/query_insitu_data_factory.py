from parquet_flask.io_logic.query_insitu_data.query_gmu_insitu_data import QueryGmuInsituData
from parquet_flask.io_logic.query_v4 import QueryV4
from parquet_flask.utils.factory_abstract import FactoryAbstract


class QueryInsituDataFactory(FactoryAbstract):
    def get_instance(self, class_type, **kwargs):
        if 'GMU' in class_type:
            return QueryGmuInsituData(query_props=kwargs['query_props'], base_url=kwargs['base_url'])
        return QueryV4(props=kwargs['query_props'])
