# About
This is a detailed guide to deploy SDAP In-Situ to AWS. Although there is a plan to use terraform for deploying SDAP In-Situ to AWS, it is still being developed and is not yet ready. Therefore, the current method for deploying SDAP In-Situ to AWS requires manual steps.

# Prerequisite/Preparation
## AWS Resources Provision
### S3
- 1 bucket storing Parquet data
- additional bucket(s) (as necessary) storing in-situ data to be ingested
### OpenSearch
- create a domain named `sdap-in-situ` ([AWS OpenSearch guide](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/gsg.html))
- under `sdap-in-situ`, create 2 indices named `entry_file_record` and `parquet_stats`
- for `entry_file_records`, create an alias named `entry_file_records_alias`
- for `parquet_stats`, create an alias named `parquet_stats_alias`
### EKS
- set up EKS in AWS environment ([AWS EKS guide](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html))
- make sure [kubectl](https://kind.sigs.k8s.io/docs/user/quick-start/) is able to communicate with EKS
- create a namespace called `nexus-in-situ`
### IAM Access Key
- create a user called `nexus-in-situ-user`
- create an access key for `nexus-in-situ-user` to be used by SDAP In-Situ
- cdms-parquet should have follow permissions
    - S3 bucket full access
    - OpenSearch full access
## Prepare SDAP In-Situ For Deployment
### Required softwares
- [docker cli](https://docs.docker.com/engine/install/) is part of docker engine installation
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
- [helm cli](https://helm.sh/docs/intro/quickstart/)
### Build Parquet Flask container
- build docker container using [parquet.spark.3.2.0.r44.Dockerfile](docker/parquet.spark.3.2.0.r44.Dockerfile)

        docker build -f docker/parquet.spark.3.2.0.r44.Dockerfile -t {your-tag} .

### Push docker container
- push docker container to a docker container registry

        docker push {your-tag}

# Deployment
## edit `values.yaml`
- spark helm chart is a dependency to SDAP In-Situ, and its values can be updated under `bitnami` YAML block
- Update values in [values.yaml](k8s_spark/parquet.spark.helm/values.yaml) to match AWS resources provisioned from above
- sample values.yaml with explanations follows (more value options and explanation can be found inside `values.yaml`)

        flask_env:
            parquet_file_name: "S3 URL for the bucket created in the first step. Note that `s3a` needs to be used. s3a://cdms-dev-in-situ-parquet/CDMS_insitu.parquet"
            spark_app_name: "any name of the spark app name. example: parquet_flask"
            log_level: "python3 log level. DEBUG, INFO, and etc."
            flask_prefix: "prefix to SDAP In-Situ API"
            es_url: "URL to AWS OpenSearch"
            es_port: "AWS OpenSearch port number (default to 443)"
            spark_config_dict: {"spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"} Change to a `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider` if IAM based credentials are used. But it is not tested at this moment.
    
        aws_creds:
            awskey: "long term aws Key. leave an empty string if using IAM"
            awssecret: "long term aws secret. leave an empty string if using IAM"
            awstoken: "aws session if using locally for a short lived aws credentials. leave an empty string if using IAM"

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

## Deploy SDAP In-Situ and Spark Cluster to k8s namespace: `nexus-in-situ`
- From the [helm folder](k8s_spark/parquet.spark.helm), run this command

        helm install -n nexus-in-situ --dependency-update parquet-t1 .

- After deploying, this is what kubernetes should look like: `kubectl get all -n nexus-in-situ`

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

- command to proxy forward to SDAP In-Situ port (if needed)

        kubectl port-forward service/parquet-t1-parquet-spark-helm -n bitnami-spark 9801:9801

### Querying Parquet via Flask
- Example command:
        
        curl 'http://localhost:30801/1.0/query_data_doms?startIndex=3&itemsPerPage=20&minDepth=-99&variable=wind_speed&columns=air_pressure&maxDepth=-1&startTime=2019-02-14T00:00:00Z&endTime=2021-02-16T00:00:00Z&platform=3B&bbox=-111,11,111,99'

# References
- [Bitnami Spark Helm Chart values](https://github.com/bitnami/charts/tree/master/bitnami/spark)
- [AWS IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up-enable-IAM.html)
- [AWS EKS guide](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html)
- [kubectl quick start guide](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [docker cli](https://docs.docker.com/engine/install/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
- [helm cli](https://helm.sh/docs/intro/quickstart/)