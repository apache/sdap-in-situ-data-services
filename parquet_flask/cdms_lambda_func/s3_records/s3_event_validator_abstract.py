from abc import ABC, abstractmethod


class S3EventValidatorAbstract(ABC):

    def __init__(self, event) -> None:
        super().__init__()

    @abstractmethod
    def size(self):
        return

    @abstractmethod
    def get_s3_url(self, index: int):
        return

    @abstractmethod
    def get_event_name(self, index: int):
        return
