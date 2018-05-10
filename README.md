# xqa-ingest-balancer [![Build Status](https://travis-ci.org/jameshnsears/xqa-ingest-balancer.svg?branch=master)](https://travis-ci.org/jameshnsears/xqa-ingest-balancer) [![Coverage Status](https://coveralls.io/repos/github/jameshnsears/xqa-ingest-balancer/badge.svg?branch=master)](https://coveralls.io/github/jameshnsears/xqa-ingest-balancer?branch=master)
* evenly distributes the ingested XML across one or more shards.

![High Level Design](https://github.com/jameshnsears/xqa-documentation/blob/master/uml/ingest-balancer-sequence-diagram.jpg)

## 1. Build

### 1.1. Maven
* rm -rf $HOME/.m2/*
* mvn package -DskipTests

### 1.2. Docker
* docker-compose -p "dev" build --force-rm

## 2. Bring up
* docker-compose -p "dev" up -d xqa-message-broker

### 3. Test

### 3.1. Maven
* mvn clean compile test
* mvn jacoco:report coveralls:report
* mvn site  # findbugs

### 3.2. CLI
* java -jar target/xqa-ingest-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar -message_broker_host 127.0.0.1 -pool_size 4

or

* POOL_SIZE=4 docker-compose -p "dev" up -d

## 4. Teardown
* docker-compose -p "dev" down -v
