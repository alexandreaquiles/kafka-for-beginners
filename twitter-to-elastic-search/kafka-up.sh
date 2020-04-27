#!/bin/bash

zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

kafka-server-start.sh $KAFKA_HOME/config/server.properties &
