# xqa-ingest-balancer [![Build Status](https://travis-ci.org/jameshnsears/xqa-ingest-balancer.svg?branch=master)](https://travis-ci.org/jameshnsears/xqa-ingest-balancer) [![Coverage Status](https://coveralls.io/repos/github/jameshnsears/xqa-ingest-balancer/badge.svg?branch=master)](https://coveralls.io/github/jameshnsears/xqa-ingest-balancer?branch=master)
* evenly distributes the ingested XML across one or more shards.

## 1. High Level Design
![High Level Design](https://github.com/jameshnsears/xqa-documentation/blob/master/uml/balancer-sequence-diagram.jpg)

## 2. Maven
### 2.1. Clean .m2
* rm -rf $HOME/.m2/*

### 2.2. Test - needs xqa-manager to be running!
* mvn clean compile test
* mvn jacoco:report coveralls:report

### 2.3. Package
* mvn package -DskipTests

### 2.4. Run
* java -jar target/xqa-ingest-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar

## 3. Docker
### 3.1. Build locally
* docker-compose -p "dev" build --force-rm

or

* mvn clean install dockerfile:build

### 3.2. Bring up
* docker-compose -p "dev" up -d

### 3.3. Teardown
* docker-compose -p "dev" down --rmi all -v
