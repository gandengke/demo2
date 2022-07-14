#!/usr/bin/env bash
inputArg=$1
basePath=$(cd `dirname $0`; pwd)
arr=(${inputArg//#...#/ })
appName=${arr[0]}
args=${arr[1]}
drivercores=${2}
drivermemory=${3}
executornum=${4}
executorcore=${5}
executormem=${6}
shufflepar=${7}
echo "drivercores="${drivercores}"dirvermemory="${drivermemory}",executornum="${executornum}",executorcore="${executorcore}",executormem="${executormem}
spark-submit \
    --master yarn \
    --name ${appName} \
    --driver-cores ${drivercores} \
    --driver-memory ${drivermemory} \
    --num-executors ${executornum} \
    --executor-cores ${executorcore} \
    --executor-memory ${executormem} \
    --conf spark.sql.shuffle.partitions=${shufflepar} \
    --conf spark.driver.maxResultSize=5GB \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.shuffle.service.enabled=true \
    --class com.jd.easy.audience.task.dataintegration.run.spark.DataIntegrationServiceDriven \
    $basePath/../task-dataintegration-sass.jar \
    ${args}
