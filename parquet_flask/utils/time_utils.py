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
import calendar
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from time import mktime

LOGGER = logging.getLogger(__name__)


class TimeUtils:
    REG_IS_NUMERIC = re.compile("^\d+?\.\d+?$")
    DOY = re.compile("^\d{4}-\d{3}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?$")
    MMDD = re.compile("^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?$")

    DOY_FORMAT = '%Y-%jT%H:%M:%S'
    MMDD_FORMAT = '%Y-%m-%dT%H:%M:%S'
    GB_1 = 1000000000
    YR_IN_SECOND = 31536000

    def __init__(self):
        self.__time_obj = datetime.utcnow()

    def parse_from_str(self, timestamp_str: str, fmt='%Y-%m-%dT%H:%M:%S%z', in_ms=False):
        self.__time_obj = datetime.strptime(timestamp_str, fmt)
        return self

    def parse_from_unix(self, unix_timestamp, in_ms=False):
        converting_timestamp = unix_timestamp / 1000 if in_ms is True else unix_timestamp
        self.__time_obj = datetime.fromtimestamp(converting_timestamp, timezone(timedelta(0, 0, 0, 0)))
        return self

    def get_datetime_obj_prop(self):
        return self.__time_obj

    def get_datetime_unix(self, in_ms=False):
        return int(self.__time_obj.timestamp()) if not in_ms else int(self.__time_obj.timestamp() * 1000)

    def get_datetime_str(self, fmt='%Y-%m-%dT%H:%M:%S %z', in_ms=True):
        return self.__time_obj.strftime(fmt).replace('0000', '00:00')
    @staticmethod
    def get_current_year():
        return datetime.utcnow().year

    @staticmethod
    def is_smaller(target, source):
        """
        if left is smaller than right

        :param target: str or int - the target value
        :param source: str or int - the source value
        :return: bool - if left is smaller than right
        """
        if isinstance(target, str):
            target = TimeUtils.encode_datetime(target)
        if isinstance(source, str):
            source = TimeUtils.encode_datetime(source)
        return target < source

    @staticmethod
    def is_between(target, smaller, larger):
        """
        if target is between smaller and larger

        NOTE: smaller and larger are not validated

        :param target: str or int - the value to be compared
        :param smaller: str or int - the smaller value
        :param larger: str or int - the larger value
        :return: bool - if target is between small and larger value
        """
        if isinstance(target, str):
            target = TimeUtils.encode_datetime(target)
        if isinstance(smaller, str):
            smaller = TimeUtils.encode_datetime(smaller)
        if isinstance(larger, str):
            larger = TimeUtils.encode_datetime(larger)
        return smaller <= target <= larger

    @staticmethod
    def get_current_unix_milli():
        return int(datetime.now().timestamp() * 10**3)

    @staticmethod
    def get_current_unix_nano():
        """
        Get the current unix time in nanosecond.

        Note-1: datetime.utcnow() will get the current utc.
                but datetime.utcnow().timestamp() will assume take utc time as input,
                it will assumes it is in current timezone, and convert to utc.
                Hence, only datetime.now() is needed.
        Note-2: need to convert to int. if not, it returns in float form.
        :return:
        """
        return int(datetime.now().timestamp() * 10**9)

    @staticmethod
    def get_unix_from_timestamp(str_timestamp):
        if TimeUtils.REG_IS_NUMERIC.match(str_timestamp):
            return str_timestamp
        try:
            dt = datetime.strptime(str_timestamp, '%Y-%jT%H:%M:%S.%f')  # yyyy-DDD'T'HH:mm:ss.SSS
        except Exception as e:
            LOGGER.error('Date conversion failed for %s' % str_timestamp)
            raise e

        assert not dt.tzinfo, "Datetime stamps must use UTC time."
        return mktime(dt.timetuple())

    @staticmethod
    def reduce_to_ms(unix_time):
        return divmod(unix_time, 10 ** 6)[0]

    @staticmethod
    def decode_datetime(unix_time, doy=True):
        """
        convert unix time (milliseconds) to YYYY-DDDTHH:mm:ss.SSS in UTC
        it assumes that the unix time is in milliseconds.
        :param unix_time: unix time in millisecond
        :param doy: decoding in DOY format? defaulted it to True
        :return: YYYY-DDDTHH:mm:ss.SSS or YYYY-MM-DDTHH:mm:SS
        """
        str_format = TimeUtils.DOY_FORMAT if doy is True else TimeUtils.MMDD_FORMAT
        return '{0}.{1:09d}'.format(datetime.utcfromtimestamp(unix_time // TimeUtils.GB_1).strftime(str_format),
                                    unix_time % TimeUtils.GB_1)
        pass

    @staticmethod
    def __append_zeros(nanosecond):
        if len(nanosecond) == 9:  # already in nanosecond format
            return nanosecond

        if len(nanosecond) > 9:  # greater than nanosecond?
            return nanosecond[0:9]  # for now, we only accept nanosecond

        return nanosecond + ''.zfill(9 - len(nanosecond))

        pass

    @staticmethod
    def encode_datetime(to_be_parsed):
        """
        convert datetime string to unix time.
        Assumption: the unix time will be in NANOseconds
        accepted formats
        'YYYY-DDDTHH:mm:ss.SZ' S can vary from 1 - 9
        'YYYY-DDDTHH:mm:ss.SSS'
        'YYYY-DDDTHH:mm:ss.SSSSSSZ'
        'YYYY-DDDTHH:mm:ss.SSSSSS'
        'YYYY-DDDTHH:mm:ss'
        calendar.timegm will convert the string datetime to UTC time.
        But it will not include millisecond.
        Temporary workaround is to manually add them.
        :param to_be_parsed: datetime in string to be parsed
        :return: unix epoch including millisecond
        """
        if TimeUtils.REG_IS_NUMERIC.match(to_be_parsed):  # if it is double, return as it is
            return to_be_parsed

        if to_be_parsed.endswith('Z') or to_be_parsed.endswith('z'):  # ending with Z, remove it
            to_be_parsed = to_be_parsed[:-1]

        if TimeUtils.DOY.match(to_be_parsed):  # Day-Of-year format
            matching_format = '%Y-%jT%H:%M:%S.%f'
        elif TimeUtils.MMDD.match(to_be_parsed):  # MM-dd format
            matching_format = '%Y-%m-%dT%H:%M:%S.%f'
        else:  # not supported yet
            LOGGER.error('Date conversion failed for %s' % to_be_parsed)
            return None

        nanosecond = 0
        if '.' in to_be_parsed:  # it has floating points
            split_milli = to_be_parsed.split('.')  # split to datetime & floating points
            if len(split_milli) == 2:  # if the result array has 2,
                nanosecond += int(TimeUtils.__append_zeros(split_milli[1]))
            # if there are more than 6 decimal values, the `time` library cannot handle it.
            # replacing the fraction with 0
            to_be_parsed = split_milli[0] + '.0'
        else:  # it has no floating points
            to_be_parsed += '.0'  # adding 0 nanosecond for regex

        try:
            return calendar.timegm(time.strptime(to_be_parsed, matching_format)) * TimeUtils.GB_1 \
                   + nanosecond
        except Exception as e:
            LOGGER.error('Date conversion failed for {}. exception: {}'.format(to_be_parsed, e))
            return None

    @staticmethod
    def get_current_time(doy_format=False, include_time=True, include_fraction=True):
        output_format = '%Y-%j' if doy_format is True else '%Y-%m-%d'
        if include_time:
            output_format = '{}T%H:%M:%S'.format(output_format)
            if include_fraction:
                output_format = '{}.%f'.format(output_format)
        return datetime.utcnow().strftime(output_format)

    @staticmethod
    def get_current_time_str() -> str:
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    @staticmethod
    def get_time_str(unix_timestamp, fmt='%Y-%m-%dT%H:%M:%SZ', in_ms=True) -> str:
        converting_timestamp = unix_timestamp / 1000 if in_ms is True else unix_timestamp
        return datetime.utcfromtimestamp(converting_timestamp).strftime(fmt)

    @staticmethod
    def get_current_time_unix() -> int:
        return int(datetime.utcnow().timestamp() * 1000)

    @staticmethod
    def get_datetime_obj(dt_str, fmt='%Y-%m-%dT%H:%M:%SZ') -> datetime:
        return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
