#!/bin/sh
ZIP=`pwd`/plugins/cassandra/build/distributions/elasticsearch-cassandra-0.13.0-SNAPSHOT.zip
PLUGINS=`pwd`/build/distributions/exploded/plugins
CASSANDRA=$PLUGINS/cassandra
#./gradlew :explodedDist
./gradlew :plugins-cassandra:release
mkdir -p $CASSANDRA
cd $CASSANDRA && unzip -u $ZIP
