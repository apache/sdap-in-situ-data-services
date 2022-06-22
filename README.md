# Insitu Data in Parquet format stored in S3

### How to ingest a insitu json file to Parquet
- Assumption: K8s is successfully deployed
- Download this repo
- (optional) create different python3.6 environment
- install dependencies

        python3 setup.py install
- setup AWS tokens
    
        export AWS_ACCESS_KEY_ID=xxx
        export AWS_SECRET_ACCESS_KEY=xxx
        export AWS_SESSION_TOKEN=really.long.token
        export AWS_REGION=us-west-2
    - alternatively the `default` profile under `~/.aws/credentials` can be setup as well
- setup current directory to `PYTHONPATH`
        
        PYTHONPATH="${PYTHONPATH}:/absolute/path/to/current/dir/"
- run the script: 

        python3 -m parquet_cli.ingest_s3 --help
    - sample script:
    
            python3 -m parquet_cli.ingest_s3 \
              --LOG_LEVEL 30 \
              --CDMS_DOMAIN https://doms.jpl.nasa.gov/insitu  \
              --CDMS_BEARER_TOKEN Mock-CDMS-Flask-Token  \
              --PARQUET_META_TBL_NAME cdms_parquet_meta_dev_v1  \
              --BUCKET_NAME cdms-dev-ncar-in-situ-stage  \
              --KEY_PREFIX cdms_icoads_2017-01-01.json
  
### Ref:
- how to replace parquet file partially
```
https://stackoverflow.com/questions/38487667/overwrite-specific-partitions-in-spark-dataframe-write-method?noredirect=1&lq=1
> Finally! This is now a feature in Spark 2.3.0: SPARK-20236
> To use it, you need to set the spark.sql.sources.partitionOverwriteMode setting to dynamic, the dataset needs to be partitioned, and the write mode overwrite. Example:
> https://stackoverflow.com/questions/50006526/overwrite-only-some-partitions-in-a-partitioned-spark-dataset

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
data.toDF().write.mode("overwrite").format("parquet").partitionBy("date", "name").save("s3://path/to/somewhere")

```