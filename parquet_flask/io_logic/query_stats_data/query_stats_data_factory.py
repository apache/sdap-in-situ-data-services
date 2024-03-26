from parquet_flask.io_logic.query_stats_data.query_gmu_stats_insitu import QueryGmuStatsData
from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics
from parquet_flask.utils.factory_abstract import FactoryAbstract


class QueryStatsDataFactory(FactoryAbstract):
    def get_instance(self, class_type, **kwargs):
        if 'GMU' in class_type:
            return QueryGmuStatsData(query_props=kwargs['query_props'])
        return SubCollectionStatistics(query_props=kwargs['query_props'])
