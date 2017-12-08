FROM debian:latest

MAINTAINER james.hn.sears@gmail.com

RUN apt-get -qq update
RUN apt-get -qq install -y openjdk-8-jre

ARG OPTDIR=/opt
ARG XQA=/xqa-shard

RUN mkdir -p ${OPTDIR}${XQA}
COPY target/xqa-ingest-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar ${OPTDIR}${XQA}

RUN useradd -r -M -d ${OPTDIR}${XQA} xqa
RUN chown -R xqa:xqa ${OPTDIR}${XQA}

USER xqa

WORKDIR ${OPTDIR}${XQA}

ENTRYPOINT ["java", "-jar", "xqa-ingest-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar"]
