from parquet_flask.io_logic.query_insitu_data.query_insitu_abstract import QueryInsituAbstract
from parquet_flask.io_logic.query_v2 import QueryProps


class QueryGmu(QueryInsituAbstract):
    def __init__(self, query_props: QueryProps) -> None:
        super().__init__(query_props)

    def search(self, spark_session=None):
        # if self._query_props.platform_id is None:
        # if no platform ID, search platforms
        # if platforms, call them directly
        return {
            'total': 0,
            'results': [],
        }