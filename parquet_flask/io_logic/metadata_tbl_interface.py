import abc


class MetadataTblInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def insert_record(self, new_record):
        return

    @abc.abstractmethod
    def get_by_s3_url(self, s3_url):
        return

    @abc.abstractmethod
    def get_by_uuid(self, uuid):
        return

    @abc.abstractmethod
    def query_by_date_range(self, start_time, end_time):
        return
