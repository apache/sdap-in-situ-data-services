from abc import ABC, abstractmethod
from typing import Union


class AuthenticatorAbstract(ABC):
    @abstractmethod
    def get_auth_credentials(self, cred_name: str) -> dict:
        return {}

    @abstractmethod
    def authenticate(self, input_auth_cred: dict) -> Union[str, None]:
        return None
