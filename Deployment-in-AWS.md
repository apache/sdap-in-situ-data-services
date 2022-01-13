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

### Deploy Spark Cluster
- We are using the bitnami spark deployment in Kubernetes
- We can use the custom values in [k8s_spark/k8s_spark/values.yaml](k8s_spark/k8s_spark/values.yaml)
- By default values, Spark master can be accessed inside the `VPC` as they `NodePort` is being used.
- AWS Loadbalancer setting was not used as it would open up Spark APIs to the Internet. 

        helm install custom-spark bitnami/spark  -n bitnami-spark --dependency-update -f k8s_spark/values.yaml
- After installation, we can view it by running this command: `kubectl get all -n bitnami-spark`

            NAME                        READY   STATUS    RESTARTS   AGE
            pod/custom-spark-master-0   1/1     Running   0          43h
            pod/custom-spark-worker-0   1/1     Running   0          43h
            pod/custom-spark-worker-1   1/1     Running   0          43h
            pod/custom-spark-worker-2   1/1     Running   0          43h
            pod/custom-spark-worker-3   1/1     Running   0          43h
            
            NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                       AGE
            service/custom-spark-headless     ClusterIP   None             <none>        <none>                        43h
            service/custom-spark-master-svc   NodePort    10.108.174.225   <none>        7077:30901/TCP,80:31151/TCP   43h
            
            NAME                                   READY   AGE
            statefulset.apps/custom-spark-master   1/1     43h
            statefulset.apps/custom-spark-worker   4/4     43h


### Build Parquet Flask container
- build it using this [Dockerfile](k8s_spark/parquet.spark.3.2.0.r44.Dockerfile)
- Note that the image name and tag needs to be updated. 

        docker build -f parquet.spark.3.2.0.r44.Dockerfile -t waiphyojpl/cdms.parquet.flask:t7 ..
        
### Deploy Parquet Flask container to kubernetes namespace: `bitnami-spark`
- Helm charts are created to deploy the flask container to the same namespace. 
- [values.yaml](k8s_spark/parquet.spark.helm/values.yaml) should be copied to another location so that default values can be updated.
- This is the sample values.yaml with explanations

        flask_env:
          parquet_file_name: "S3 URL for the bucket created in the first step. Note that `s3a` needs to be used. s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet"
          master_spark_url: "master spark URL.  spark://custom-spark-master-0.custom-spark-headless.bitnami-spark.svc.cluster.local:7077"
          spark_app_name: "any name of the spark app name. example: parquet_flask_demo"
          log_level: "python3 log level. DEBUG, INFO, and etc."
          parquet_metadata_tbl: "DynamoDB table name: cdms_parquet_meta_dev_v1"
          spark_config_dict: {"spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"} Change to a `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider` if IAM based credentials are used. But it is not tested at this moment.
        
        aws_creds:
          awskey: 'long term aws Key. leave an empty string if using IAM'
          awssecret: 'long term aws secret. leave an empty string if using IAM'
          awstoken: 'aws session if using locally for a short lived aws credentials. leave an empty string if using IAM'
        image:
          repository: "the name value from `Build Parquet Flask container` step. example: waiphyojpl/cdms.parquet.flask"
          pullPolicy: IfNotPresent
          # Overrides the image tag whose default is the chart appVersion.
          tag: "the tag value from `Build Parquet Flask container` step. example: t7"

- from the [helm folder](k8s_spark/parquet.spark.helm), run this command

        helm install parquet-t1 . -n bitnami-spark -f <custom path to values.yaml>
- after deploying this: this is what kuberntes look like: `kubectl get all -n bitnami-spark`

        NAME                                                 READY   STATUS    RESTARTS   AGE
        pod/custom-spark-master-0                            1/1     Running   0          29d
        pod/custom-spark-worker-0                            1/1     Running   0          29d
        pod/custom-spark-worker-1                            1/1     Running   0          29d
        pod/custom-spark-worker-2                            1/1     Running   0          29d
        pod/custom-spark-worker-3                            1/1     Running   0          29d
        pod/parquet-t1-parquet-spark-helm-57dc976bff-ctgqw   1/1     Running   0          8d
        
        NAME                                    TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                       AGE
        service/custom-spark-headless           ClusterIP   None             <none>        <none>                        29d
        service/custom-spark-master-svc         NodePort    10.100.241.197   <none>        7077:32131/TCP,80:31140/TCP   29d
        service/parquet-t1-parquet-spark-helm   NodePort    10.100.25.165    <none>        9801:30801/TCP                8d
        
        NAME                                            READY   UP-TO-DATE   AVAILABLE   AGE
        deployment.apps/parquet-t1-parquet-spark-helm   1/1     1            1           8d
        
        NAME                                                       DESIRED   CURRENT   READY   AGE
        replicaset.apps/parquet-t1-parquet-spark-helm-57dc976bff   1         1         1       8d
        
        NAME                                   READY   AGE
        statefulset.apps/custom-spark-master   1/1     29d
        statefulset.apps/custom-spark-worker   4/4     29d
- The parquet port can be forwarded if needed. There are plans to use `ingress` or `AWS Loadbalancer`, but they are still in development as SA hasn't approved it yet.

        kubectl port-forward service/parquet-t1-parquet-spark-helm -n bitnami-spark 9801:9801

### Querying Parquet via Flask
- Example command:
        
        time curl 'http://localhost:30801/1.0/query_data_doms?startIndex=3&itemsPerPage=20&minDepth=-99&variable=wind_speed&columns=air_pressure&maxDepth=-1&startTime=2019-02-14T00:00:00Z&endTime=2021-02-16T00:00:00Z&platform=3B&bbox=-111,11,111,99'

