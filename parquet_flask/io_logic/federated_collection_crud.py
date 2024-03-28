from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config
from parquet_flask.utils.time_utils import TimeUtils


class FederatedCollectionCrud:
    def __init__(self):
        config = Config()
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=CDMSConstants.federated_collections,
                                                         base_url=config.get_value(Config.es_url),
                                                         port=int(config.get_value(Config.es_port, '443')))

    def get(self):
        result = self.__es.query_pages({
            'query': {
                'match_all': {}
            }
        })
        return [k['_source'] for k in result['hits']['hits']]

    def insert(self, provider, project):
        self.__es.index_one({
            'provider': provider,
            'project': project,
            'event_time': TimeUtils.get_current_time_unix()
        }, f'{provider}___{project}')
        return

    def update(self, provider, project):
        self.__es.update_one({
            'provider': provider,
            'project': project,
            'event_time': TimeUtils.get_current_time_unix()
        }, f'{provider}___{project}')
        return

    def delete(self, provider, project):
        self.__es.delete_by_query({
            'query': {
                'bool': {
                    'must': [
                        {'term': {'provider': {'value': provider}}},
                        {'term': {'project': {'value': project}}},
                    ]
                }
            }
        })
        return


