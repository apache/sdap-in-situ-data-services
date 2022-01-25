FROM bitnami/spark:3.1.2-debian-10-r70

USER root
RUN apt-get update -y && apt-get install vim -y
RUN mkdir /usr/app
WORKDIR /usr/app
COPY parquet_flask /usr/app/parquet_flask
COPY setup.py /usr/app
COPY in_situ_schema.json /usr/app

RUN python3 /usr/app/setup.py install
ENV PYTHONPATH="${PYTHONPATH}:/usr/app/"
ENV in_situ_schema=/usr/app/in_situ_schema.json

RUN echo '{"auth_cred":"Mock-CDMS-Flask-Token"}' > /usr/app/cdms_flask_auth.json
ENV authentication_key '/usr/app/cdms_flask_auth.json'
ENV authentication_type 'FILE'

CMD python3 -m parquet_flask