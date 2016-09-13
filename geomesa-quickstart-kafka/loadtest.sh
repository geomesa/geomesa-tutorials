#!/bin/sh

VERSION=1.2.5.1-SNAPSHOT

java -Xmx4g -cp target/geomesa-quickstart-kafka-$VERSION.jar \
    com.example.geomesa.kafka.KafkaLoadTester \
	-brokers localhost:9092 \
	-zookeepers localhost:2181 \
	-count 100000

