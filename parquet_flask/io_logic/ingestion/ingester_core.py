from parquet_flask.io_logic.ingestion.ingest_props import IngestProps
from parquet_flask.io_logic.ingestion.json_ingester_plugin import JsonIngesterPlugin
from parquet_flask.io_logic.ingestion.nc_ingester_plugin import NcIngesterPlugin
from parquet_flask.io_logic.ingestion.publish_result_plugin import PublishResultPlugin
from parquet_flask.io_logic.ingestion.update_meta_table_plugin import UpdateMetaTablePlugin


class IngesterCore:
    TYPE_JSON = 'TYPE_JSON'
    TYPE_NC = 'TYPE_NC'

    def __init__(self, ingesting_type: str, props: IngestProps):
        self.__props = props
        self.__ingesting_type = ingesting_type
        self.__plug_ins = [
            self.__get_ingester_plugin(),
            UpdateMetaTablePlugin(self.__props),
            PublishResultPlugin(self.__props),
        ]

    def __get_ingester_plugin(self):
        if self.__ingesting_type == self.TYPE_JSON:
            return JsonIngesterPlugin(self.__props)
        if self.__ingesting_type == self.TYPE_NC:
            return NcIngesterPlugin(self.__props)
        raise ValueError(f'unknown ingesting_type: {self.__ingesting_type}')

    def start(self):
        for each_plugin in self.__plug_ins:
            each_plugin.ingest()
        return
