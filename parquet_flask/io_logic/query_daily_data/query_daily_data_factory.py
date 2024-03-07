from parquet_flask.io_logic.insitu_records_to_es import InsituRecordsToEs
from parquet_flask.io_logic.query_daily_data.gmu_insitu import GmuInsitu
from parquet_flask.utils.factory_abstract import FactoryAbstract


class QueryDailyDataFactory(FactoryAbstract):
    def get_instance(self, class_type, **kwargs):
        if 'GMU' in class_type:
            return GmuInsitu()
        return InsituRecordsToEs(es_url=kwargs['es_url'])
