from datetime import datetime


class TimeUtils:
    @staticmethod
    def get_current_time_str():
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    @staticmethod
    def get_current_time_unix():
        return int(datetime.utcnow().timestamp() * 1000)

    @staticmethod
    def get_datetime_obj(dt_str, fmt='%Y-%m-%dT%H:%M:%SZ'):
        return datetime.strptime(dt_str, fmt)

    @staticmethod
    def get_time_str(unix_timestamp, fmt='%Y-%m-%dT%H:%M:%SZ', in_ms=True):
        converting_timestamp = unix_timestamp / 1000 if in_ms is True else unix_timestamp
        return datetime.utcfromtimestamp(converting_timestamp).strftime(fmt)
