from parquet_flask.aws.ddb_middleware import DDBMiddleware, DDBMiddlewareProps
from parquet_flask.io_logic.cdms_constants import CDMSConstants


class MetadataTblIO:
    """
    Table columns
        - s3_url
        - uuid
        - size
        - ingested_date
        - md5
        - num-of-records

        Table settings
        s3_url as primary key
        secondary index: uuid
        secondary index: ingested_date
    """
    def __init__(self):
        ddb_props = DDBMiddlewareProps()
        ddb_props.hash_key = CDMSConstants.s3_url_key
        ddb_props.tbl_name = 'cdms_parquet_meta_dev_v1'  # TODO come from config
        self.__ddb = DDBMiddleware(ddb_props)

    def insert_record(self, new_record):
        self.__ddb.add_one_item(new_record, new_record[CDMSConstants.s3_url_key])
