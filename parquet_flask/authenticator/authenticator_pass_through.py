from typing import Union

from parquet_flask.authenticator.authenticator_abstract import AuthenticatorAbstract


class AuthenticatorPassThrough(AuthenticatorAbstract):
    def get_auth_credentials(self, cred_name: str) -> dict:
        return {}

    def authenticate(self, input_auth_cred: dict) -> Union[str, None]:
        return None
