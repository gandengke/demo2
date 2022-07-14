#!/usr/bin/env bash
inputArg=$1
basePath=$(cd `dirname $0`; pwd)
#arr=(${inputArg//#...#/ })
appName=$1
time=$2
args=$3
spark-submit \
    --master yarn \
    --name ${appName} \
    --conf spark.sql.shuffle.partitions=800 \
    --driver-memory  1g \
    --executor-cores  2 \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --class com.jd.easy.audience.task.plugin.step.readdata.EventLogParseForLz4 \
    $basePath/../task-plugin-sass.jar \
    ${time} ${args}
