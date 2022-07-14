#!/usr/bin/env bash
appName=$1
basePath=$(cd `dirname $0`; pwd)
echo ${day},${startMinute},${endMinute}
spark-submit \
    --master yarn \
    --name ${appName} \
    --conf spark.sql.shuffle.partitions=800 \
    --driver-memory  2g \
    --executor-cores  4 \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --class com.jd.easy.audience.task.dataintegration.run.spark.CataTableInfoUpdateStep \
    $basePath/../task-dataintegration-sass.jar \
