from abc import ABC, abstractmethod

from parquet_flask.io_logic.ingestion.ingest_props import IngestProps


class IngestPluginAbstract(ABC):
    def __init__(self, props: IngestProps) -> None:
        super().__init__()
        self._props = props

    @abstractmethod
    def ingest(self):
        return self
