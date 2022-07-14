#!/usr/bin/env bash
inputArg=$1
basePath=$(cd `dirname $0`; pwd)
arr=(${inputArg//#...#/ })
appName=${arr[0]}
args=${arr[1]}
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
    --class com.jd.easy.audience.task.dataintegration.run.spark.DataIntegrationServiceDriven \
    $basePath/../task-dataintegration-sass.jar \
    ${args}
