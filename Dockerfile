FROM ubuntu:bionic

MAINTAINER james.hn.sears@gmail.com

RUN apt-get -qq update
RUN apt-get -qq install -y openjdk-10-jre

RUN apt-get install --reinstall -y locales
RUN sed -i 's/# en_GB.UTF-8 UTF-8/en_GB.UTF-8 UTF-8/' /etc/locale.gen
RUN locale-gen en_GB.UTF-8
ENV LANG en_GB.UTF-8
ENV LANGUAGE en_GB
ENV LC_ALL en_GB.UTF-8
RUN dpkg-reconfigure --frontend noninteractive locales

ARG OPTDIR=/opt
ARG XQA=/xqa-ingest-balancer

RUN mkdir -p ${OPTDIR}${XQA}
COPY target/xqa-ingest-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar ${OPTDIR}${XQA}

RUN useradd -r -M -d ${OPTDIR}${XQA} xqa
RUN chown -R xqa:xqa ${OPTDIR}${XQA}

USER xqa

WORKDIR ${OPTDIR}${XQA}

ENTRYPOINT ["java", "-jar", "xqa-ingest-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar"]
