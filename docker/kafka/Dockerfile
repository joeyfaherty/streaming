# Dockerfile for Kafka
FROM alpine:latest
MAINTAINER joeyfaherty[at]live.ie
ARG KAFKA_VERSION=0.10.2.0

ENV SCALA_VERSION 2.11
ENV URL=https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz

WORKDIR /kafka

RUN set -euxo pipefail \
  && apk add --no-cache bash openjdk8-jre-base wget tar

RUN set -euxo pipefail \
  && wget -t 10 --max-redirect 1 --retry-connrefused -O "kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz" "${URL}" \
  && tar zxf "kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz" -C /kafka --strip 1

COPY start-kafka /bin/
COPY server.properties /kafka/config/

VOLUME /kafka/logs
EXPOSE 9092

ENV KAFKA_CONFIG /kafka/config/server.properties
ENV PATH=$PATH:/kafka/bin
ENV ZOOKEEPER_CONNECT zookeeper-joey
ENV BROKER_ID 0
ENV BROKER_PORT 9092
ENV ADVERTISED_HOST_NAME kafka-local
ENV ADVERTISED_PORT 9092
ENV REPLICATION_FACTOR 1
ENV TOPIC_PARTITIONS 4

CMD ["start-kafka"]
