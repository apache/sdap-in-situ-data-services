from parquet_flask.utils.factory_abstract import FactoryAbstract


class ESFactory(FactoryAbstract):
    NO_AUTH = 'NO_AUTH'
    AWS = 'AWS'

    def get_instance(self, class_type, **kwargs):
        ct = class_type.upper()
        if ct == self.NO_AUTH:
            from parquet_flask.aws.es_middleware import ESMiddleware
            return ESMiddleware(kwargs['index'], kwargs['base_url'], port=kwargs['port'])
        if ct == self.AWS:
            from parquet_flask.aws.es_middleware_aws import EsMiddlewareAws
            return EsMiddlewareAws(kwargs['index'], kwargs['base_url'], port=kwargs['port'])
        raise ModuleNotFoundError(f'cannot find ES class for {ct}')
