from functools import wraps

from flask import request

from parquet_flask.authenticator.authenticator_abstract import AuthenticatorAbstract
from parquet_flask.authenticator.authenticator_factory import AuthenticatorFactory
from parquet_flask.utils.config import Config


def authenticator_decorator(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        config: Config = Config()
        authenticator: AuthenticatorAbstract = AuthenticatorFactory().get_instance(config.get_value(config.authentication_type, AuthenticatorFactory.FILE))
        try:
            authenticator.get_auth_credentials(config.get_value(config.authentication_key, 'None'))
            auth_result = authenticator.authenticate(request.headers)
            if auth_result is not None:
                return {'message': auth_result}, 403
        except Exception as e:
            return {'message': f'failed while attempting to authenticate', 'details': str(e)}, 500
        return f(*args, **kwargs)
    return decorated_function
