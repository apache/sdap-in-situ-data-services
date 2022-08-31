FROM public.ecr.aws/lambda/python:3.7

#USER root
#RUN apt-get update -y && apt-get install vim -y
RUN mkdir /usr/app
WORKDIR /usr/app

COPY setup_lambda.py /usr/app
RUN python3 /usr/app/setup_lambda.py install
ENV PYTHONPATH="${PYTHONPATH}:/usr/app/"

COPY parquet_flask /usr/app/parquet_flask

COPY in_situ_schema.json /etc
ENV insitu_schema_file=/etc/in_situ_schema.json

CMD ["parquet_flask.cdms_lambda_func.index_to_es.execute_lambda.execute_code"]
