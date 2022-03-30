FROM bitnami/spark:3.2.0-debian-10-r44

USER root
RUN apt-get update -y && apt-get install vim -y
RUN mkdir /usr/app
WORKDIR /usr/app

COPY setup.py /usr/app
RUN python3 /usr/app/setup.py install
ENV PYTHONPATH="${PYTHONPATH}:/usr/app/"

RUN echo '{"auth_cred":"Mock-CDMS-Flask-Token"}' > /usr/app/cdms_flask_auth.json
ENV authentication_key '/usr/app/cdms_flask_auth.json'
ENV authentication_type 'FILE'

COPY parquet_flask /usr/app/parquet_flask

COPY in_situ_schema.json /usr/app
ENV in_situ_schema=/usr/app/in_situ_schema.json

CMD python3 -m parquet_flask