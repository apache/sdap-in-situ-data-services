import logging
import os
import sys

from parquet_flask.cdms_lambda_func.lambda_func_env import LambdaFuncEnv


class LambdaLoggerGenerator:
    @staticmethod
    def remove_default_handlers():
        root_logger = logging.getLogger()
        for each in root_logger.handlers:
            root_logger.removeHandler(each)
        return

    @staticmethod
    def get_level_from_env():
        return int(os.environ.get(LambdaFuncEnv.LOG_LEVEL, logging.INFO))

    @staticmethod
    def get_logger(logger_name: str, log_level: int = logging.INFO, log_format: str = None):
        if log_format is None:
            log_format = LambdaFuncEnv.LOG_FORMAT
        new_logger = logging.getLogger(logger_name)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter(log_format))
        stream_handler.setLevel(log_level)
        new_logger.setLevel(log_level)
        new_logger.addHandler(stream_handler)
        return new_logger
