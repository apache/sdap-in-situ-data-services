### Kubernetes Deployment
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

#### Notes on enabling EKS IRSA (IAM Roles for Service Accounts)

1. The WebIdentityTokenCredentialsProvider is required.
```
flask_env:
  spark_config_dict: {
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
  }
```
2. Set your IAM Role ARN as an annotation within the `parquet-t1` service account
```
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: 'arn:aws:iam::xxxxxxxxxxxxxx:role/parquet-spark'
```
3. Reuse the same service account for your Bitnami Spark deployment. \[note: If desired, you can also utilize independent service accounts.  Just be sure to bind the Bitnami Spark service account to either the same IAM Role or a unique IAM Role with similar permissions.\]
```
  serviceAccount:
    create: false
    name: "parquet-t1-parquet-spark-helm"
```
4. Bitnami Spark security context dissallows access to the authentication token.  Disabling the security context is the will resolve this. \[note: It's also possible that enabling the security context with `fsGroup: 65534` would enable filesystem access and be more secure, though this is, as of yet, untested.\]
```
  securityContext:
    enabled: false
    #   fsGroup: 1001
    #   runAsUser: 1001
    #   runAsGroup: 0
    #   seLinuxOptions: {}
```
5. Leave `aws_creds` yaml block commented out.
```
# AWS EKS IRSA is favored over AWS IAM User credentials when possible
# Uncomment to enable IAM User Credential authentication
# aws_creds:
#   awskey: "xxxxxx"
#   awssecret: "xxxxxx"
#   awstoken: "xxxxxx"
```