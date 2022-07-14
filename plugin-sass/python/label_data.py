#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import random
import sys
import hashlib
import datetime
import subprocess
import os
sys.path.append(os.getenv('HIVE_TASK'))

from HiveTask import HiveTask

ht = HiveTask(log_lev='WARN')


# 文件大小1M
fileSize = 1*1024*1024

fieldDelimiter = ','
dirPath=os.getcwd()
dbName = sys.argv[1]
sqlCond = """
	SELECT pin FROM """+dbName+""".app_dm_ae_pin_tb
;
"""
extendname=""
if(sys.argv[2]=='phone'):
    sqlCond = """
	SELECT phone FROM """+dbName+""".app_dm_ae_phone_tb
;"""
    extendname="_phone"
else:
    extendname="_pin"
create_sql="""
create table IF NOT EXISTS """+dbName+""".tmp_dm_ae_label_feature"""+extendname+"""_tb( 
    user_id string COMMENT 'user id', 
    sex string COMMENT '性别', 
	  age string COMMENT '年龄', 
	  os_plant string COMMENT '移动设备操作系统', 
	  career string COMMENT '职业', 
	  addr_level string COMMENT '收获地址城市等级', 
	  memship_level string COMMENT '会员级别', 
	  amount_avg string COMMENT '平均购买金额', 
	  last_arrive string COMMENT '最后访问时间')

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
create table IF NOT EXISTS """+dbName+""".app_dm_ae_label_feature"""+extendname+"""_tb( 
    user_id string COMMENT 'user id', 
    sex string COMMENT '性别', 
	  age string COMMENT '年龄', 
	  os_plant string COMMENT '移动设备操作系统', 
	  career string COMMENT '职业', 
	  addr_level string COMMENT '收获地址城市等级', 
	  memship_level string COMMENT '会员级别', 
	  amount_avg string COMMENT '平均购买金额', 
	  last_arrive string COMMENT '最后访问时间')

STORED AS ORC;
"""
##创建表
# cmd_create = 'hive -e """'+create_sql.replace('"', "\'")+'"""'
# print(cmd_create)
# p = subprocess.Popen(cmd_create, shell=True, stdout=subprocess.PIPE)
cmd = 'hive -e """'+sqlCond.replace('"', "\'")+'"""'
print("cmd=" + cmd)
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
cnt=0
userArr=[]
while True:
    buff = p.stdout.readline();
    buff = buff.decode('utf-8').strip()
    if buff == '':
        print("filished");
        break;
    else:
        cnt=cnt+1

        paramArr = buff.split('\t')
        userArr.append(paramArr[0])
    ##ht.exec_sql(schema_name = 'app', table_name = 'app_fc_upath_pinspv', sql = sqlnew)

print("cnt=", len(userArr))
cnt=len(userArr)
# cnt=20000000
print("ok")
# 50个枚举以上、空值、特殊字符
# date range
def dateRange(start, end, step=1, format="%Y-%m-%d"):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days
    return [strftime(strptime(start, format) + datetime.timedelta(i), format) for i in range(0, days, step)]
# gender
def gender():
    items=['男', '女']
    return random.choice(items)
# os
def os():
    items=['IOS', 'Android', 'Windows phone', '其他']
    return random.choice(items)
# age
def age():
    items=['15岁以下', '16-20岁', '21-25岁', '26-30岁', '31-35岁', '36-40岁', '41-45岁', '46-50岁', '51-55岁', '56-60岁', '61-65岁', '66-70岁', '70岁以上']
    return random.choice(items)
# user level
def userLevel():
    items=['注册会员', '铜牌会员', '银牌会员', '金牌会员', '钻石会员']
    return random.choice(items)
def career():
    items=['个体经营/服务业', '公司职员', '工人', '公务员', '医务人员', '学生', '教职工', '其他']
    return random.choice(items)
def addrlevel():
    items=['直辖市', '省会城市', '省直辖市', '省直辖县', '地级市']
    return random.choice(items)
def amountavg():
    items=['10元以下', '10-50元', '50-100元', '100-200元', '200-300元', '300-500元', '500-1000元', '1000元以上']
    return random.choice(items)
#最后一次访问时间
def lastaccess():
    items=['n<=30天', '30天<n<=60天', '60天<n<=90天', '90天<n<=180天', 'n>180天']
    return random.choice(items)
# order num
def orderNum():
    return str(int(random.randrange(1, 10)))
# user id
def userId(index):
    return userArr[index].strip()
#imei
def imei():
    return ''.join(str(random.choice(range(10))) for _ in range(15))
# mac
def mac():
    enum=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
    num_enum = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    return random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum)
# md5 phone
totalSize = 0
progress = 0
sizeOfEachDate = cnt
print("Start to Create Data ...")
start_day=datetime.datetime.now().strftime('%Y-%m-%d')
end_day=datetime.datetime.now().strftime('%Y-%m-%d')

rowLoop = 0

basicFileName = "./userLabelBasicData.txt"
basicWriter = open(basicFileName, 'w')
extFileName = "./userLabelExtData.txt"
extWriter = open(extFileName, 'w')
pathBasic=dirPath + "/userLabelBasicData.txt"
pathExt=dirPath + "/userLabelExtData.txt"
while rowLoop < sizeOfEachDate:
    userIdStr = userId(rowLoop)
    # userIdStr = "user_" + str(rowLoop)
    basicContent = userIdStr + fieldDelimiter + gender() + fieldDelimiter + age() \
                   + fieldDelimiter + os()  + fieldDelimiter + career() \
                   + fieldDelimiter + addrlevel() + fieldDelimiter + userLevel() \
                   + fieldDelimiter + amountavg()  + fieldDelimiter + lastaccess()
    basicContent += '\n'
    basicWriter.write(basicContent)
    rowLoop += 1
basicWriter.close()
extWriter.close()
sqlinsert="""
        LOAD DATA LOCAL INPATH '"""+pathBasic+"""' OVERWRITE  INTO TABLE """+dbName+""".tmp_dm_ae_label_feature_pin_tb;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set mapreduce.job.reduces=5;
        set hive.merge.mapfiles = true;
        set hive.merge.mapredfiles = true;
        set hive.merge.size.per.task = 256000000;
        set hive.merge.smallfiles.avgsize=128000000;
        set hive.merge.orcfile.stripe.level=false;
        INSERT OVERWRITE TABLE """+dbName+""".label_feature_phone_demo
        SELECT
            user_id,
            sex,
            age,
            os_plant,
            career,
            addr_level,
            memship_level,
            amount_avg,
            last_arrive
        FROM """+dbName+""".tmp_dm_ae_label_feature_pin_tb;
        """
print(sqlinsert)
# ht.exec_sql(schema_name = dbName, table_name = "app_dm_ae_label_feature"+extendname+"_tb",
# sql = sqlinsert, merge_flag = True, merge_type='mr')
ht.exec_sql(schema_name = dbName, table_name = "label_feature_phone_demo",
            sql = sqlinsert)
print("Complete.")