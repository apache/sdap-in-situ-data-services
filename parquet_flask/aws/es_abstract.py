from abc import ABC, abstractmethod
from typing import Any, Union, Callable

DEFAULT_TYPE = '_doc'


class ESAbstract(ABC):
    @abstractmethod
    def create_index(self, index_name, index_body):
        return

    @abstractmethod
    def index_many(self, docs=None, doc_ids=None, doc_dict=None, index=None):
        return

    @abstractmethod
    def index_one(self, doc, doc_id, index=None):
        return

    @abstractmethod
    def update_many(self, docs=None, doc_ids=None, doc_dict=None, index=None):
        return

    @abstractmethod
    def update_one(self, doc, doc_id, index=None):
        return

    @staticmethod
    @abstractmethod
    def get_result_size(result):
        return

    @abstractmethod
    def query_with_scroll(self, dsl, querying_index=None):
        return

    @abstractmethod
    def query(self, dsl, querying_index=None):
        return

    @abstractmethod
    def query_pages(self, dsl, querying_index=None):
        return

    @abstractmethod
    def query_by_id(self, doc_id):
        return

    @abstractmethod
    def delete_by_id(self, doc_id, index=None):
        return

    @abstractmethod
    def delete_by_query(self, dsl, index=None):
        return
