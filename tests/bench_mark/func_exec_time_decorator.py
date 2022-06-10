import logging
from datetime import datetime
from functools import wraps

LOGGER = logging.getLogger(__name__)


def func_exec_time_decorator(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        time1 = datetime.now()
        func_result = f(*args, **kwargs)
        time2 = datetime.now()
        duration = time2 - time1
        LOGGER.info(f'duration: {duration.total_seconds()} s. name: {f.__name__}')
        return func_result, duration.total_seconds(), {'time1': time1.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'time2': time2.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
    return decorated_function
