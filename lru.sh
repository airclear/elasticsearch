#!/bin/sh
ZIP=`pwd`/plugins/client/lru/build/distributions/elasticsearch-client-lru-0.13.0-SNAPSHOT.zip
PLUGINS=`pwd`/build/distributions/exploded/plugins
LRU=$PLUGINS/lruclient
#./gradlew :explodedDist
./gradlew :plugins-client-lru:release
mkdir -p $LRU
cd $LRU && unzip -u $ZIP
