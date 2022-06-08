FROM python:alpine3.14

USER root

COPY . /opt/


RUN apk add --update --no-cache build-base python3-dev python3 libffi-dev libressl-dev postgresql-dev curl aws-cli


RUN pip install regex requests psycopg2-binary pytz  elementpath pandas numpy  tinys3


