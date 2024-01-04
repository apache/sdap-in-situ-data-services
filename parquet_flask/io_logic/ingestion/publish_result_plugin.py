import json
import logging

from parquet_flask.aws.pub_sub_factory import PubSubFactory
from parquet_flask.io_logic.ingestion.ingest_plugin_abstract import IngestPluginAbstract
LOGGER = logging.getLogger(__name__)


class PublishResultPlugin(IngestPluginAbstract):
    def ingest(self):
        if self._props.pub_sub_topic is None:
            LOGGER.debug(f'missing pub_sub_topic. not continuing')
            return
        pub_sub = PubSubFactory().get_instance('SNS').set_channel(self._props.pub_sub_topic)
        pub_sub.publish_msg(json.dumps({'s3_url': self._props.s3_url}))
        return
