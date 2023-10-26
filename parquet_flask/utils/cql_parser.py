import json

from pygeofilter.ast import Attribute, And, Equal, Or, IsNull, In, LessThan, GreaterEqual, GreaterThan, LessEqual
from pygeofilter.parsers.cql2_text import parse as text_parse

# https://github.com/geopython/pygeofilter/tree/main
# https://github.com/geopython/pygeofilter/blob/main/pygeofilter/examples/cql2.ipynb
# https://github.com/geopython/pygeofilter/blob/main/tests/parsers/cql2_json/fixtures.json
class CqlParser:
    def __init__(self, key_prefix=''):
        self.__key_prefix = f'{key_prefix}.' if key_prefix != '' else ''
        self.__something = {}

    def __valide_core_expression(self, parsed_obj):
        if isinstance(parsed_obj.rhs, Attribute):
            raise ValueError(f'not accepting right hand side to be another field. {parsed_obj.rhs}')
        return

    def __transform_this(self, parsed_obj):
        if isinstance(parsed_obj, And):
            return {
                'bool': {
                    'must': [
                        self.__transform_this(parsed_obj.lhs),
                        self.__transform_this(parsed_obj.rhs),
                    ]
                }
            }
        if isinstance(parsed_obj, Or):
            return {
                'bool': {
                    'should': [
                        self.__transform_this(parsed_obj.lhs),
                        self.__transform_this(parsed_obj.rhs),
                    ]
                }
            }
        if isinstance(parsed_obj, IsNull):
            return {
                'must_not': [{
                    'exists': f'{self.__key_prefix}{parsed_obj.lhs.name}'
                }]
            }
        if isinstance(parsed_obj, Equal):
            self.__valide_core_expression(parsed_obj)
            return {
                'term': {f'{self.__key_prefix}{parsed_obj.lhs.name}': parsed_obj.rhs},  # TODO it rhs might be an attribute as well
            }
        if isinstance(parsed_obj, In):
            return {
                'terms': {f'{self.__key_prefix}{parsed_obj.lhs.name}': parsed_obj.sub_nodes},  # TODO it rhs might be an attribute as well
            }
        if isinstance(parsed_obj, LessThan):
            self.__valide_core_expression(parsed_obj)
            return {
                'range': {f'{self.__key_prefix}{parsed_obj.lhs.name}': {'lt': parsed_obj.rhs}},
            }
        if isinstance(parsed_obj, LessEqual):
            self.__valide_core_expression(parsed_obj)
            return {
                'range': {f'{self.__key_prefix}{parsed_obj.lhs.name}': {'lte': parsed_obj.rhs}},
            }
        if isinstance(parsed_obj, GreaterThan):
            self.__valide_core_expression(parsed_obj)
            return {
                'range': {f'{self.__key_prefix}{parsed_obj.lhs.name}': {'gt': parsed_obj.rhs}},
            }
        if isinstance(parsed_obj, GreaterEqual):
            self.__valide_core_expression(parsed_obj)
            return {
                'range': {f'{self.__key_prefix}{parsed_obj.lhs.name}': {'gte': parsed_obj.rhs}},
            }
        return

    def transform(self, parsed_text):
        parsed_obj = text_parse(parsed_text)
        return self.__transform_this(parsed_obj)