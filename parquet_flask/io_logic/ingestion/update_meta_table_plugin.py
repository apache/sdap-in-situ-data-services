import logging

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.ingestion.ingest_plugin_abstract import IngestPluginAbstract
from parquet_flask.io_logic.metadata_tbl_es import MetadataTblES
from parquet_flask.io_logic.metadata_tbl_interface import MetadataTblInterface

LOGGER = logging.getLogger(__name__)


class UpdateMetaTablePlugin(IngestPluginAbstract):
    def ingest(self):
        if self._props.generated_record is None:
            LOGGER.debug(f'missing generated_record. not continuing')
            return
        es: ESAbstract = ESFactory().get_instance('AWS', index='', base_url=self._props.es_url, port=self._props.es_port)
        db_io: MetadataTblInterface = MetadataTblES(es)

        LOGGER.debug(f'uploading to metadata table')
        if self._props.is_replacing:
            db_io.replace_record(self._props.generated_record)
        else:
            db_io.insert_record(self._props.generated_record)
        return
