#!/usr/bin/env bash
inputArg=$1
basePath=$(cd `dirname $0`; pwd)
spark-submit \
    --master yarn \
    --name CDP_SPARK_HDFS2HIVETEST \
    --conf spark.sql.shuffle.partitions=800 \
    --driver-memory  1g \
    --executor-cores  2 \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --class com.jd.easy.audience.task.dataintegration.run.spark.Hdfs2HiveProcess \
    $basePath/../task-dataintegration-sass.jar \
    ${inputArg} $2 $3 $4