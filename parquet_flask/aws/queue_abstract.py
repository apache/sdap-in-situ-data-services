from abc import ABC, abstractmethod

from parquet_flask.aws.queue_service_props import QueueServiceProps


class QueueAbstract(ABC):
    def __init__(self, props: QueueServiceProps):
        self._props = props

    @abstractmethod
    def send_msg(self, body: dict):
        return