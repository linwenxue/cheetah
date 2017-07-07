#!/bin/bash
pp=$(dirname $0)
cd $pp
pw=$(pwd)

count_time=$1
if [ ! -n "$count_time" ]; then
  count_time=$(date -d '1 hour ago' +%Y%m%d%H)
fi
echo $count_time

SPARK_HOME='/myspark'
$SPARK_HOME/bin/spark-submit --master yarn --executor-cores 1 --driver-class-path $SPARK_HOME/lib/mysql-connector-java-5.1.36-bin.jar  --jars $SPARK_HOME/lib/mysql-connector-java-5.1.36-bin.jar  --num-executors 2 --executor-memory 2g --driver-memory 1g etl-cheetah-1.0-SNAPSHOT.jar -c config.properties -t $count_time