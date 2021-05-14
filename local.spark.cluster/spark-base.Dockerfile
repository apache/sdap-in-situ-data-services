FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.0.0
ARG hadoop_version=2.7

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

RUN apt install vim -y

COPY ./aws-java-sdk-1.7.4.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/
COPY ./hadoop-aws-2.7.4.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/
COPY ./spark-defaults.conf /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/conf/
# /usr/bin/spark-3.0.0-bin-hadoop2.7/jars

ENV spark_version=3.0.0
ENV hadoop_version=2.7
ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Runtime

WORKDIR ${SPARK_HOME}