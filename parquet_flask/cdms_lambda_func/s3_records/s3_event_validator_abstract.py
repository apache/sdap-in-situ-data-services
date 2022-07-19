from abc import ABC, abstractmethod


class S3EventValidatorAbstract(ABC):

    def __init__(self, event) -> None:
        super().__init__()

    @abstractmethod
    def get_s3_url(self):
        return

    @abstractmethod
    def get_event_name(self):
        return
