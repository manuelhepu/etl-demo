FROM python:alpine3.14

USER root

COPY . /opt/


RUN apk add --update --no-cache build-base python3-dev python3 libffi-dev libressl-dev  curl aws-cli





