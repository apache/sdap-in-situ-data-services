#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Copyright 2023, by the California Institute of Technology. ALL RIGHTS RESERVED.
#  United States Government Sponsorship acknowledged. Any commercial use must be
#  negotiated with the Office of Technology Transfer at the California Institute of
#  Technology.  This software is subject to U.S. export control laws and regulations
#  and has been classified as EAR99.  By accepting this software, the user agrees to
#  comply with all applicable U.S. export laws and regulations.  User has the
#  responsibility to obtain export licenses, or other export authority as may be
#  required before exporting such information to foreign countries or providing
#  access to foreign persons.
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import logging

from parquet_flask.aws.pub_sub_sns import PubSubSns
from parquet_flask.utils.factory_abstract import FactoryAbstract

LOGGER = logging.getLogger(__name__)


class PubSubFactory(FactoryAbstract):
    def get_instance(self, pub_sub_type: str, **kwargs):
        fr = pub_sub_type.upper()
        if fr == 'SNS':
            return PubSubSns()
        raise ModuleNotFoundError(f'cannot find FileStream class for {fr}')
