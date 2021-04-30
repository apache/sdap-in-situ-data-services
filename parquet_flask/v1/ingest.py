import json
import logging
import os
import uuid
from multiprocessing import Process

from flask_restx import Resource, Namespace, fields
from flask import request

from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile

api = Namespace('ingest_json', description="Ingesting JSON files")


@api.route('', methods=["put"])
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self.__saved_dir = '/tmp'  # TODO update this

    def __get_time_col(self, request_files):
        if 'time_col' not in request_files:
            return None
        return request_files['time_col']

    def __get_partition_list(self, request_files):
        if 'partitions' not in request_files:
            return []
        partitions_str = request_files['partitions']
        try:
            return json.loads(partitions_str)
        except:
            return []

    @api.expect()
    def put(self):
        """
        s3://ecsv-h5-data-v1/INDEX/GALILEO/filenames.txt.gz

        :return:
        """
        ss = request
        if 'file' not in request.files:
            return {'message': 'No file part in the request'}, 400
        file_obj = request.files['file']

        if file_obj.filename == '':
            return {'message': 'No file selected for uploading'}, 400
        if os.path.splitext(file_obj.filename)[1].lower() != '.json':
            return {'message': 'invalid file extension. Only json files are allowed'}, 400
        saved_file_name = '/tmp/{}'.format(file_obj.filename)
        file_obj.save(saved_file_name)
        try:
            IngestNewJsonFile().ingest(saved_file_name, time_col=self.__get_time_col(request.form),
                                       partitions=self.__get_partition_list(request.form))
            return {'message': 'ingested'}, 201
        except Exception as e:
            return {'message': 'failed to ingest to parquest', 'details': str(e)}, 500
