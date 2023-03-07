# This file should be executed from the path (context) '../../' e.g.:
# """
# cd ../../
# sudo docker build -t psi/graph-discovery -f Dockerfile .
# """

FROM ubuntu:20.04

WORKDIR /code

USER root
RUN apt-get update
RUN apt-get install -y python3-pip

COPY ./sdk/util/requirements_kafka.txt .
COPY ./search-service/pipeline-controller/requirements.txt .
RUN pip install -U -r requirements.txt
RUN pip install -U -r requirements_kafka.txt

COPY ./sdk/util/AbstractMessageBrokerHandler.py ./util/AbstractMessageBrokerHandler.py
COPY ./sdk/util/cloudEventMessage.py ./util/cloudEventMessage.py
COPY ./search-service/pipeline-controller/requirements.txt .
COPY ./search-service/pipeline-controller/*.py ./
COPY ./search-service/package/*.py ./package/
COPY ./search-service/package/taxonomies/* ./taxonomies/