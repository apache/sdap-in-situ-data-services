These instructions apply to kubernetes deployment only.  For full instructions on how to install within AWS, please visit the [Deployment-in-AWS] (https://github.com/apache/incubator-sdap-in-situ-data-services/blob/master/Deployment-in-AWS.md) doc.

From this directory: `/parquet_test_1/k8s_spark/parquet.spark.helm`
- Update values in `values.yaml` to match your environment (dynamodb table name, s3 bucket name, etc.)
- Run this to install the parquet spark

        helm install parquet-t1 . -n bitnami-spark --dependency-update

- Port forward the `service` to access it from outside

        kubectl port-forward service/parquet-t1-parquet-spark-helm -n bitnami-spark 9801:9801

- Run this to upgrade parquet spark after values changes

        helm upgrade parquet-t1 . -n bitnami-spark

- Run this to uninstall the parquet spark

        helm uninstall parquet-t1 -n bitnami-spark
