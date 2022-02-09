### Current Data Structure
- Apache Spark is used to store data to S3.
- There are 8 executors in Apache Spark
- Data is partitioned by several columns.
- Code Snippet: 

        @staticmethod
        def create_df(spark_session, data_list, job_id, provider, project):
            LOGGER.debug(f'creating data frame with length {len(data_list)}')
            df = spark_session.createDataFrame(data_list)
            LOGGER.debug(f'adding columns')
            df = df.withColumn(CDMSConstants.time_obj_col, to_timestamp(CDMSConstants.time_col))\
                .withColumn(CDMSConstants.year_col, year(CDMSConstants.time_col))\
                .withColumn(CDMSConstants.month_col, month(CDMSConstants.time_col))\
                .withColumn(CDMSConstants.platform_code_col, df[CDMSConstants.platform_col][CDMSConstants.code_col])\
                .withColumn(CDMSConstants.job_id_col, lit(job_id))\
                .withColumn(CDMSConstants.provider_col, lit(provider))\
                .withColumn(CDMSConstants.project_col, lit(project))
                # .withColumn('ingested_date', lit(TimeUtils.get_current_time_str()))
            LOGGER.debug(f'create writer')
            df_writer = df.write
            all_partitions = [CDMSConstants.provider_col, CDMSConstants.project_col, CDMSConstants.platform_code_col,
                              CDMSConstants.year_col, CDMSConstants.month_col, CDMSConstants.job_id_col]
            LOGGER.debug(f'create partitions')
            df_writer = df_writer.partitionBy(all_partitions)
            LOGGER.debug(f'created partitions')
            return df_writer
- Data is stored using default Spark and Hadoop parameters one of which is the following setting

        s3a.fs.block.size = 30M
- result structure in S3:

        aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/year=2017/month=10/job_id=fb1e78a6-1518-4c2c-b588-f5d769b61ab8/'
        2022-01-27 18:32:24      19906 part-00000-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:20      18968 part-00001-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:23      19298 part-00002-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:21      18339 part-00003-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:18      17298 part-00004-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:17      17173 part-00005-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:16      17500 part-00006-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
        2022-01-27 18:32:22      19111 part-00007-93aa95e4-0b08-4eb0-8a3f-56c4a63f7bab.c000.gz.parquet
- despite the default setting of `s3a.fs.block.size = 30M`, the resulting files are only ~20KB in size
- Executed a test without any partitioning, increasing the block size to 1GB, and switching compression from GZIP to snappy  did not help. 
- Code snippet:

        local_file_path = s3.set_s3_url('s3://cdms-dev-fsu-in-situ-stage/KAOU_20170222.json.gz').download('/tmp')
        local_file_path = FileUtils.gunzip_file_os(local_file_path)
        with open(local_file_path, 'r') as f:
            data_list = json.loads(f.read())
        df = spark.createDataFrame(data_list['observations'])
        df_writer = df.write   # .partitionBy(['hour'])
        df_writer.mode('append').parquet('s3a://cdms-dev-in-situ-parquet/back_to_basis', compression='snappy')  # snappy GZIP
- result structure in S3:

        [wphyo@ip-172-31-31-6 ~]$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/back_to_basis/'
        2022-02-03 16:53:50          0 _SUCCESS
        2022-02-03 16:53:46      25202 part-00000-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:39      24812 part-00001-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:41      24861 part-00002-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:44      22968 part-00003-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:35      19574 part-00004-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:37      23067 part-00005-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:48      22542 part-00006-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
        2022-02-03 16:53:42      25157 part-00007-aff155ea-1985-4850-aabc-070d4ca532d0-c000.snappy.parquet
### Current Performance
- code snippet for the query:

        read_df: DataFrame = spark.read.parquet(self.__parquet_name)
        read_df_time = datetime.now()
        LOGGER.debug(f'parquet read created at {read_df_time}. duration: {read_df_time - created_spark_session_time}')
        query_result = read_df.where(conditions)
        query_result = query_result
        query_time = datetime.now()
        LOGGER.debug(f'parquet read filtered at {query_time}. duration: {query_time - read_df_time}')
        LOGGER.debug(f'total duration: {query_time - query_begin_time}')
        total_result = int(query_result.count())
        LOGGER.debug(f'total calc count duration: {datetime.now() - query_time}')
        if self.__props.size < 1:
            LOGGER.debug(f'returning only the size: {total_result}')
            return {
                'total': total_result,
                'results': [],
            }
        query_time = datetime.now()
        removing_cols = [CDMSConstants.time_obj_col, CDMSConstants.year_col, CDMSConstants.month_col]
        selected_columns = self.__get_selected_columns()
        if len(selected_columns) > 0:
            query_result = query_result.select(selected_columns)
        LOGGER.debug(f'returning size : {total_result}')
        result = query_result.limit(self.__props.start_at + self.__props.size).drop(*removing_cols).tail(self.__props.size)
- `conditions` are a huge string of `AND` statements like this:

        platform_code == '30' AND provider == 'Florida State University, COAPS' AND project == 'SAMOS' AND year >= 2017 AND time_obj >= '2017-12-01T00:00:00Z' AND year <= 2017 AND time_obj <= '2017-12-16T00:00:00Z' AND month >= 12 AND month <= 12 AND latitude >= 11.0 AND longitude >= -111.0 AND latitude <= 99.0 AND longitude <= 111.0 AND ((depth >= -99.0 AND depth <= 0.0) OR depth == -99999) AND (relative_humidity IS NOT NULL)
- based on the logging statements, these are the rough durations

        2022-02-04 19:53:53,760 [DEBUG] [parquet_flask.io_logic.query_v2::426] spark session created at 2022-02-04 19:53:53.760736. duration: 0:00:06.766588
        2022-02-04 19:56:46,312 [DEBUG] [parquet_flask.io_logic.query_v2::429] parquet read created at 2022-02-04 19:56:46.312092. duration: 0:02:52.551356
        2022-02-04 19:56:46,465 [DEBUG] [parquet_flask.io_logic.query_v2::433] parquet read filtered at 2022-02-04 19:56:46.465474. duration: 0:00:00.153382
        2022-02-04 19:56:46,465 [DEBUG] [parquet_flask.io_logic.query_v2::434] total duration: 0:02:59.471326
        2022-02-04 20:06:40,575 [DEBUG] [parquet_flask.io_logic.query_v2::437] total calc count duration: 0:09:54.110272
        2022-02-04 20:06:53,108 [DEBUG] [parquet_flask.io_logic.query_v2::454] total retrieval duration: 0:00:12.532355
- According to the Spark UI, there are around 60
### Investigation
- According to Spark UI, there are 20 - 60 jobs like this:

        Listing leaf files and directories for 511 paths:
        s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/year=2017/month=5/job_id=0045542e-85ff-4273-a257-3b379c59e57a, ...
        parquet at NativeMethodAccessorImpl.java:0
- Each of them takes around 2s, and when viewing the details, all 8 executors are executing on it. 
- The more jobs like these, the slower `spark.read.parquet` is.
- They are reading all files from `s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30` even though the query has `year <= 2017 and year >= 2017`.
    - Probably we should update this to be `year = XXX`
- `total calc count` takes a long time. According to some articles from google, it is expected, 
    - Is there a better way to do it than `query_result.count()`
### Potential Solution
- Currently `self.__parquet_name` in `read_df: DataFrame = spark.read.parquet(self.__parquet_name)` is `s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet`.
- `provider=Florida State University, COAPS/project=SAMOS/platform_code=30` is added by `Spark` behind the scenes. 
- Manually adding `s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/year=2017` would reduce the `Listing leaf files` jobs
    - but `conditions` need to be updated to remove `pvovider`, `project`, and etc since they are no loner part of parquet. 
    - It complains if they are still there.
- remove `query_result.count()` for 2nd page onward to speed it up.