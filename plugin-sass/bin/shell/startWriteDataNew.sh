#!/usr/bin/env bash
endpoint=$1
accessKey=$2
secretKey=$3
bucket=$4
osspath=$5
fileformat=$6
basePath=$(cd `dirname $0`; pwd)
echo $basePath
spark-submit \
    --master yarn \
    --name dataPluginServiceDriven \
    --conf spark.speculation=true \
    --conf spark.scheduler.mode=FAIR \
    --conf hive.exec.orc.default.stripe.size=268435456L \
    --conf hive.exec.orc.split.strategy=BI \
    --conf spark.network.timeout=900s \
    --conf spark.sql.shuffle.partitions=800 \
    --conf park.default.parallelism=600 \
    --driver-memory  5g \
    --executor-cores  2 \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --class com.jd.easy.audience.task.plugin.run.spark.WriteOssDemo \
    $basePath/../task-plugin-sass.jar \
    ${endpoint} ${accessKey} ${secretKey} ${bucket} ${osspath} ${fileformat}
