## AWS Resources
- while there is a plan to have a terraform setup for this setup, it is still in development
- Currently, they are setup manually.

### S3
- 1 bucket which stores Parquet data
### DynamoDB
- 1 table for Parquet metadata mapped to S3 file metadata
- Table setting

        Partition key: s3_url (String)
        Sort key: -
        Capacity mode: On-demand
### IAM for long term tokens or IAM role permissions
- S3 bucket full access
- Read permission for Other S3 buckets where in-situ json file are stored.
- DDB: full access for the mentioned table 
### EKS setup
- This was setup by the SA. The instruction will be added in the future.
- Note that IAM roles can be setup for EKS, but SAs have not set that up yet. 
- Long term tokens were created and given to the system which are used instead of IAM roles for EKS

### Create Namespace
- for this deployment, we are using namespace `bitnami-spark`

        kubectl create namespace bitnami-spark


### Build Parquet Flask container
- build it using this [Dockerfile](k8s_spark/parquet.spark.3.2.0.r44.Dockerfile)
- Note that the image name and tag needs to be updated. 

        docker build -f ../docker/parquet.spark.3.2.0.r44.Dockerfile -t waiphyojpl/cdms.parquet.flask:t7 ..

### Deploy Parquet Flask container and Spark Cluster to kubernetes namespace: `bitnami-spark`
- We are nesting the bitnami spark helm chart as a dependency within our parquet-spark-helm helm chart.
- Spark custom values are maintained in `values.yaml` within the `bitnami-spark` YAML block.
- With the default values, Spark master can be accessed inside the `VPC` as the `NodePort` is being used.
- Spark AWS Loadbalancer setting should only be used if internal load-balancers are used on private subnets, which would allow intra-VPC access only and block public access.
- [values.yaml](k8s_spark/parquet.spark.helm/values.yaml) should be copied to another location so that default values can be updated.
- This is the sample values.yaml with explanations

        flask_env:
          parquet_file_name: "S3 URL for the bucket created in the first step. Note that `s3a` needs to be used. s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet"
          master_spark_url: "master spark URL.  spark://custom-spark-master-0.custom-spark-headless.bitnami-spark.svc.cluster.local:7077"
          spark_app_name: "any name of the spark app name. example: parquet_flask_demo"
          log_level: "python3 log level. DEBUG, INFO, and etc."
          parquet_metadata_tbl: "DynamoDB table name: cdms_parquet_meta_dev_v1"
          spark_config_dict: {"spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"} Change to a `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider` if IAM based credentials are used. But it is not tested at this moment.
        
        aws_creds: 'Will create a secret and related env vars on the deployment. Unnecessary if AWS IRSA is being used.
          awskey: 'long term aws Key. leave an empty string if using IAM'
          awssecret: 'long term aws secret. leave an empty string if using IAM'
          awstoken: 'aws session if using locally for a short lived aws credentials. leave an empty string if using IAM'

        serviceAccount:
          create: true
          annotations: {} 'Can be used for [AWS IRSA](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up-enable-IAM.html)'.  Uncomment the line below and specify an IAM Role to enable.
            # eks.amazonaws.com/role-arn: 'arn:aws:iam::xxxxxxxxxxxxxx:role/parquet-spark'

        image:
          repository: "the name value from `Build Parquet Flask container` step. example: waiphyojpl/cdms.parquet.flask"
          pullPolicy: IfNotPresent
          # Overrides the image tag whose default is the chart appVersion.
          tag: "the tag value from `Build Parquet Flask container` step. example: t7"

        bitnami-spark: 'Default values for the Bitnami Spark helm chart.'

- From the [helm folder](k8s_spark/parquet.spark.helm), run this command

        helm install parquet-t1 . -n bitnami-spark --dependency-update -f <custom path to values.yaml>
- After deploying, this is what kubernetes should look like: `kubectl get all -n bitnami-spark`

        NAME                                                 READY   STATUS    RESTARTS   AGE
        pod/parquet-t1-bitnami-spark-master-0                1/1     Running   0          29d
        pod/parquet-t1-bitnami-spark-worker-0                1/1     Running   0          29d
        pod/parquet-t1-bitnami-spark-worker-1                1/1     Running   0          29d
        pod/parquet-t1-bitnami-spark-worker-2                1/1     Running   0          29d
        pod/parquet-t1-bitnami-spark-worker-3                1/1     Running   0          29d
        pod/parquet-t1-parquet-spark-helm-57dc976bff-ctgqw   1/1     Running   0          8d
        
        NAME                                          TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                       AGE
        service/parquet-t1-bitnami-spark-headless     ClusterIP   None             <none>        <none>                        29d
        service/parquet-t1-bitnami-spark-master-svc   NodePort    10.100.241.197   <none>        7077:32131/TCP,80:31140/TCP   29d
        service/parquet-t1-parquet-spark-helm         NodePort    10.100.25.165    <none>        9801:30801/TCP                8d
        
        NAME                                            READY   UP-TO-DATE   AVAILABLE   AGE
        deployment.apps/parquet-t1-parquet-spark-helm   1/1     1            1           8d
        
        NAME                                                       DESIRED   CURRENT   READY   AGE
        replicaset.apps/parquet-t1-parquet-spark-helm-57dc976bff   1         1         1       8d
        
        NAME                                               READY   AGE
        statefulset.apps/parquet-t1-bitnami-spark-master   1/1     29d
        statefulset.apps/parquet-t1-bitnami-spark-worker   4/4     29d
- The parquet port can be forwarded if needed. There are plans to use `ingress` or `AWS Loadbalancer`, but they are still in development as SA hasn't approved it yet.

        kubectl port-forward service/parquet-t1-parquet-spark-helm -n bitnami-spark 9801:9801

### Querying Parquet via Flask
- Example command:
        
        time curl 'http://localhost:30801/1.0/query_data_doms?startIndex=3&itemsPerPage=20&minDepth=-99&variable=wind_speed&columns=air_pressure&maxDepth=-1&startTime=2019-02-14T00:00:00Z&endTime=2021-02-16T00:00:00Z&platform=3B&bbox=-111,11,111,99'

### Documentation and Sources
- [Bitnami Spark Helm Chart](https://github.com/bitnami/charts/tree/master/bitnami/spark)
- [AWS IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up-enable-IAM.html)
