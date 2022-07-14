#!/usr/bin/env bash
endpoint=$1
mountPath=$2
outputPath=$3
basePath=$(cd `dirname $0`; pwd)
echo $basePath
echo "username:"
whoami
sudo ls
spawn su root
expect "Password:"
send 'dmcBGJ_87Xmk-YB%-$cx3cd9sxa'
expect eof
echo "username:"
whoami
echo "mount before, yum install  nfs-utils..."
sudo yum install nfs-utils -y

echo "start rpcbind"
sudo service rpcbind start

echo "mount..."
sudo mount -t nfs -o vers=3 -o noresvport 11.88.104.16:/cfs /mnt

echo "mount check..."
sudo df -h

echo "permission check.."
sudo ls -ltr /
sudo -R chmod 777 /mnt

echo "chmode after.."
sudo ls -ltr /
mountPathHdfs = "hdfs://ns1/user/cdp_biz-org.bdp.cs/cfs/sharedinput"${mountPath}
echo "mountPathHdfs:"${mountPathHdfs}
sudo hdfs dfs -put ${mountPath} ${mountPathHdfs}
su bdp_client
echo "change username:"
whoami
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
    --class com.jd.easy.audience.task.plugin.run.spark.ReadOssDemo \
    $basePath/../task-plugin-sass.jar \
    ${endpoint} ${mountPathHdfs} ${outputPath}

echo "任务执行完成"
sudo hdfs dfs -ls ${mountPath}