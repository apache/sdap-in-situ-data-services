FROM spark-base

RUN mkdir /usr/app
WORKDIR /usr/app
COPY parquet_flask /usr/app/parquet_flask
COPY setup.py /usr/app
COPY flask_server.py /usr/app
COPY in_situ_schema.json /usr/app

RUN python3 /usr/app/setup.py install
ENV PYTHONPATH="${PYTHONPATH}:/usr/app/"
ENV in_situ_schema=/usr/app/in_situ_schema.json

CMD /config/export_aws_var.sh && python3 /usr/app/flask_server.py
