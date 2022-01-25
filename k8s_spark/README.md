## How to setup spark in K8s 

1. Use this git repo: https://github.com/bitnami/charts
1. Spark repo is in this sub directory: https://github.com/bitnami/charts/tree/master/bitnami/spark
    1. spark docker image: `bitnami/spark:3.1.2-debian-10-r70` is used
    1. This docker already have AWS and Hadoop-S3A libraries needed to R/W parquet data to S3
    1. Switch the `service > type:` from `ClusterIP` to `NodePort` so that it can be accessed from outside
    1. Swtiched the `ingress > enabled:` from `false` to `true`. Not sure what the purpose is. This may be reverted.
    
1. Create the namespace: 

        kubectl create namespace bitnami-spark
1. Install the spark 
        
        helm install custom-spark bitnami/spark  -n bitnami-spark --dependency-update -f k8s_spark/values.yaml
1. After it is installed:

        kubectl get all -n bitnami-spark
    1. Something similar to this is returned
            
            qNAME                        READY   STATUS    RESTARTS   AGE
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
    1.Make sure that 1 `master` and all `worker` are running. 
    1. the `spark-master URL` is `http://localhost:30901`. `30901` comes from this line `service/custom-spark-master-svc` where it is mapped to `7077`
    1. the `spark web UI URL` is `http://localhost: 31151`. `31151` comes from this line `service/custom-spark-master-svc` where it is mapped to `80`
        1. When the application is running, you can click on the running application, and can be followed to the `application details UI` where the application details can be viewed.
1. When it's done, uninstall with this:

        helm uninstall custom-spark -n bitnami-spark
1. To enter one of the pods, use this and update accordingly:
    
        kubectl exec -it -n bitnami-spark pod/custom-spark-master-0 -- /bin/bash 
1. To check the logs, use this and update accordingly:

        kubectl logs pod/custom-spark-master-0 -n bitnami-spark --since=15m
1. Build it for k8s (choose respective spark version)

        docker build -f ../docker/parquet.spark.3.1.2.r70.Dockerfile -t parquet.spark.flask:t1 ..
        docker build -f ../docker/parquet.spark.3.2.0.r44.Dockerfile -t waiphyojpl/cdms.parquet.flask:t6 ..
        docker build -f ../docker/parquet.spark.3.2.0.r44.Dockerfile -t waiphyojpl/cdms.parquet.flask:t7 ..
        
1. create secrets for aws credentials

        echo -n "pleasechangeme" | base64
1. Deploy to kube
        kubectl create -f . -n bitnami-spark
        kubectl apply -f secret.yml -n bitnami-spark
        kubectl create -f flask-deployment.yml -n bitnami-spark
        kubectl create -f flask-service.yml -n bitnami-spark
1. Remove from kube

        kubectl delete -f . -n bitnami-spark
        kubectl delete -f flask-service.yml -n bitnami-spark
        kubectl delete -f flask-deployment.yml -n bitnami-spark
        kubectl delete -f secret.yml -n bitnami-spark
        kubectl delete -f configmap.yml -n bitnami-spark
    
1. possible s3 auth providers

        org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
1. port forward the spark for local test

        kubectl port-forward service/parquet-flask -n bitnami-spark 9801:9801 &
