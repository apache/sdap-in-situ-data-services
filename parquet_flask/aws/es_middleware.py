import logging

from elasticsearch import Elasticsearch
from parquet_flask.aws.es_abstract import ESAbstract, DEFAULT_TYPE

LOGGER = logging.getLogger(__name__)


class ESMiddleware(ESAbstract):

    def __init__(self, index, base_url, port=443) -> None:
        if any([k is None for k in [index, base_url]]):
            raise ValueError(f'index or base_url is None')
        self.__index = index
        base_url = base_url.replace('https://', '')  # hide https
        self._engine = Elasticsearch(hosts=[{'host': base_url, 'port': port}])

    def __validate_index(self, index):
        if index is not None:
            return index
        if self.__index is not None:
            return self.__index
        raise ValueError('index value is NULL')

    def __get_doc_dict(self, docs=None, doc_ids=None, doc_dict=None):
        if doc_dict is None and (docs is None and doc_ids is None):
            raise ValueError('must provide either doc dictionary or doc list & id list')
        if doc_dict is None:  # it comes as a list
            if len(docs) != len(doc_ids):
                raise ValueError('length of doc and id is different')
            doc_dict = {k: v for k, v in zip(doc_ids, docs)}
            pass
        return doc_dict

    def __check_errors_for_bulk(self, index_result):
        if 'errors' not in index_result or index_result['errors'] is False:
            return
        err_list = [[{'id': v['_id'], 'error': v['error']} for _, v in each.items() if 'error' in v] for each in
                    index_result['items']]
        if len(err_list) < 1:
            return
        LOGGER.exception('failed to add some items. details: {}'.format(err_list))
        return err_list

    def create_index(self, index_name, index_body):
        result = self._engine.indices.create(index=index_name, body=index_body, include_type_name=True)
        return result

    def index_many(self, docs=None, doc_ids=None, doc_dict=None, index=None):
        doc_dict = self.__get_doc_dict(docs, doc_ids, doc_dict)
        body = []
        for k, v in doc_dict.items():
            body.append({'index': {'__index': index, '_id': k, 'retry_on_conflict': 3}})
            body.append(v)
            pass
        index = self.__validate_index(index)
        try:
            index_result = self._engine.bulk(index=index,
                                              body=body, doc_type=DEFAULT_TYPE)
            LOGGER.info('indexed. result: {}'.format(index_result))
            return self.__check_errors_for_bulk(index_result)
        except:
            LOGGER.exception('cannot add indices with ids: {} for index: {}'.format(list(doc_dict.keys()), index))
            return doc_dict
        return

    def index_one(self, doc, doc_id, index=None):
        index = self.__validate_index(index)
        try:
            index_result = self._engine.index(index=index,
                                              body=doc, doc_type=DEFAULT_TYPE, id=doc_id)
            LOGGER.info('indexed. result: {}'.format(index_result))
            pass
        except:
            LOGGER.exception('cannot add a new index with id: {} for index: {}'.format(doc_id, index))
            return None
        return self

    def update_many(self, docs=None, doc_ids=None, doc_dict=None, index=None):
        doc_dict = self.__get_doc_dict(docs, doc_ids, doc_dict)
        body = []
        for k, v in doc_dict.items():
            body.append({'update': {'__index': index, '_id': k, 'retry_on_conflict': 3}})
            body.append({'doc': v, 'doc_as_upsert': True})
            pass
        index = self.__validate_index(index)
        try:
            index_result = self._engine.bulk(index=index,
                                             body=body, doc_type=DEFAULT_TYPE)
            LOGGER.info('indexed. result: {}'.format(index_result))
            return self.__check_errors_for_bulk(index_result)
        except:
            LOGGER.exception('cannot update indices with ids: {} for index: {}'.format(list(doc_dict.keys()),
                                                                                             index))
            return doc_dict
        return

    def update_one(self, doc, doc_id, index=None):
        update_body = {
            'doc': doc,
            'doc_as_upsert': True
        }
        index = self.__validate_index(index)
        try:
            update_result = self._engine.update(index=index,
                                                id=doc_id, body=update_body, doc_type=DEFAULT_TYPE)
            LOGGER.info('updated. result: {}'.format(update_result))
            pass
        except:
            LOGGER.exception('cannot update id: {} for index: {}'.format(doc_id, index))
            return None
        return self

    @staticmethod
    def get_result_size(result):
        if isinstance(result['hits']['total'], dict):  # fix for different datatype in elastic-search result
            return result['hits']['total']['value']
        else:
            return result['hits']['total']

    def query_with_scroll(self, dsl, querying_index=None):
        scroll_timeout = '30s'
        index = self.__validate_index(querying_index)
        dsl['size'] = 10000  # replacing with the maximum size to minimize number of scrolls
        params = {
            'index': index,
            'size': 10000,
            'scroll': scroll_timeout,
            'body': dsl,
        }
        first_batch = self._engine.search(**params)
        total_size = self.get_result_size(first_batch)
        current_size = len(first_batch['hits']['hits'])
        scroll_id = first_batch['_scroll_id']
        while current_size < total_size:  # need to scroll
            scrolled_result = self._engine.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
            scroll_id = scrolled_result['_scroll_id']
            scrolled_result_size = len(scrolled_result['hits']['hits'])
            if scrolled_result_size == 0:
                break
            else:
                current_size += scrolled_result_size
                first_batch['hits']['hits'].extend(scrolled_result['hits']['hits'])
        return first_batch

    def query(self, dsl, querying_index=None):
        index = self.__validate_index(querying_index)
        return self._engine.search(body=dsl, index=index)

    def query_pages(self, dsl, querying_index=None) -> dict:
        """

        :param dsl:
        :param querying_index:
        :return: dict | {"total": 0, "items": []}
        """
        if 'sort' not in dsl:
            raise ValueError('missing `sort` in DSL. Make sure sorting is unique')
        index = self.__validate_index(querying_index)
        dsl['size'] = 10000  # replacing with the maximum size to minimize number of scrolls
        params = {
            'index': index,
            'size': 10000,
            'body': dsl,
        }
        LOGGER.debug(f'dsl: {dsl}')
        first_batch = self._engine.search(**params)
        current_size = len(first_batch['hits']['hits'])
        total_size = current_size
        while current_size > 0:
            dsl['search_after'] = first_batch['hits']['hits'][-1]['sort']
            paged_result = self._engine.search(**params)
            current_size = len(paged_result['hits']['hits'])
            total_size += current_size
            first_batch['hits']['hits'].extend(paged_result['hits']['hits'])
        return {
            'total': len(first_batch['hits']['hits']),
            'items': first_batch['hits']['hits'],
        }

    def query_by_id(self, doc_id):
        index = self.__validate_index(None)
        dsl = {
            'query': {
                'term': {'_id': doc_id}
            }
        }
        result = self._engine.search(index=index, body=dsl)
        if self.get_result_size(result) < 1:
            return None
        return result['hits']['hits'][0]

    def delete_by_id(self, doc_id, index=None):
        index = self.__validate_index(index)
        self._engine.delete(index, doc_id)
        return

    def delete_by_query(self, dsl, index=None):
        raise NotImplementedError('not yet.')

