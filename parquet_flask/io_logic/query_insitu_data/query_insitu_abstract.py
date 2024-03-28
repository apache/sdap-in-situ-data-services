from abc import ABC, abstractmethod

from parquet_flask.io_logic.query_v2 import QueryProps


class QueryInsituAbstract(ABC):
    def __init__(self, query_props: QueryProps) -> None:
        super().__init__()
        self._query_props = query_props

    @abstractmethod
    def search(self, spark_session=None):
        return {
            'total': 0,
            'results': [],
        }
