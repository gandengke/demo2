#!/usr/bin/env bash
inputArg=$1
basePath=$(cd `dirname $0`; pwd)
echo $basePath
arr=(${inputArg//#...#/ })
appName=${arr[0]}
args=${arr[1]}
spark-submit \
    --master yarn \
    --name ${appName} \
    --conf spark.speculation=true \
    --conf spark.scheduler.mode=FAIR \
    --conf hive.exec.orc.default.stripe.size=268435456L \
    --conf hive.exec.orc.split.strategy=BI \
    --conf spark.network.timeout=900s \
    --conf spark.sql.shuffle.partitions=800 \
    --conf spark.default.parallelism=600 \
    --driver-memory  5g \
    --executor-cores  2 \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    --class com.jd.easy.audience.task.plugin.run.spark.DatamillSparkPipelineDriven \
    $basePath/../task-plugin-sass.jar \
    ${args}
