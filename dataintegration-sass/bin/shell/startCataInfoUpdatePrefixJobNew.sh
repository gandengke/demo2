#!/usr/bin/env bash
inputArg=$1
basePath=$(cd `dirname $0`; pwd)
spark-submit \
    --master yarn \
    --name DATAMILL_CDPSAAS_SOURCEMANAGE_CATAINFO_UPDATEPREFIX \
    --conf spark.sql.shuffle.partitions=800 \
    --driver-memory  2g \
    --executor-cores  4 \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --class com.jd.easy.audience.task.dataintegration.run.spark.CataTableInfoUpdatePrefixStep \
    $basePath/../task-dataintegration-sass.jar \
    $inputArg