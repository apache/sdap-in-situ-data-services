from abc import ABC, abstractmethod


class FactoryAbstract(ABC):
    @abstractmethod
    def get_instance(self, class_type, **kwargs):
        return
