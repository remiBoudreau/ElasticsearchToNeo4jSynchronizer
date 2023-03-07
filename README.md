# Message Broker

## Introduction
This repository contains the docker-compose file to setup a Kafka/Zookeeper service for the Checkmate project. All topics will be contained in here and can be found at the `.env` file. The .env file contains other static configuration varibles such as hostnames, and ports for the brokers and other services.

## Installation

Download the project as follows:

```
git clone git@bitbucket.org:psicheckmate/sdk.git
cd sdk
git checkout master
git pull origin master
```

Specify the following environment variables on the host machine or volume prior to installing in the `.env` file:

```
ZOOKEEPER_DATA_DIR
ZOOKEEPER_LOG_DIR
KAFKA_LOG_DIR
ELASTIC_SEARCH_DATA_DIR
NEO4J_DATA_DIR
```
These are required to allow persistence of Kafka data in case the pipeline needs to be brought down. 

You need to create a `.credentials` file with the user and password to be used with *NEO4J* such as the following:

``` 
NEO4J_AUTH=neo4j/test
```

Then:

```docker-compose -f ./docker-compose.yml up -d --build```

## Testing

To test the SDK, you need to setup the Kafka environment with the ability to delete topics as they will be cleaned up at the end of testing. This can be done by setting up the environment variable as follows:

```
KAFKA_DELETE_TOPIC_ENABLE: "true"
```

To run the test, you need to execute the following command:

```
python3 -m unittest test.kafkaPipelineTest
```


