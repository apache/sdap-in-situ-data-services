# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    def query_by_id(self, doc_id, index=None):
        return

    @abstractmethod
    def delete_by_id(self, doc_id, index=None):
        return

    @abstractmethod
    def delete_by_query(self, dsl, index=None):
        return
