# parquet_test_1

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