#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import subprocess
import datetime
import time
import os
import random
import hashlib

sys.path.append(os.getenv('HIVE_TASK'))

from HiveTask import HiveTask

ht = HiveTask(log_lev='WARN')
print("start")
print(ht.data_day_str)
print(os.getcwd())
dbName=""
dirPath=os.getcwd()
fileName = "./rfmuserDataxm.txt"
filePath = dirPath + "/rfmuserDataxm.txt"
start_day=datetime.datetime.now().strftime('%Y-%m-%d')
end_day=datetime.datetime.now().strftime('%Y-%m-%d')
if (len(sys.argv)==6):
    start_day=sys.argv[1]
    end_day=sys.argv[2]
    dbName=sys.argv[5]
dirPath=os.getcwd()
sqlCond = """
	SELECT pin FROM """+dbName+""".app_dm_ae_pin_tb
;
"""
extendname=""
if(sys.argv[4]=='phone'):
    sqlCond = """
	SELECT phone FROM """+dbName+""".app_dm_ae_phone_tb
;
"""
    extendname="_phone"
else:
    extendname="_pin"
if(sys.argv[3]=='user'):
    create_sql="""create table IF NOT EXISTS """+dbName+""".tmp_dm_ae_rfm_user"""+extendname+"""_tb( 
                user_id string COMMENT 'user id',  
                latest_trade_date date COMMENT '最近一次消费时间',  
                trade_count int COMMENT '累计消费次数',  
                trade_money_summary double COMMENT '累计消费金额',  
                order_qtty bigint COMMENT '累计消费件数',  
                channel string COMMENT '下单渠道',  
                first_trade_date date COMMENT '第一次消费时间',  
                sku string COMMENT 'sku',  
                brand_favor string COMMENT '品牌偏好',  
                cate_favor string COMMENT '品类偏好',  
                cancel_times int COMMENT '订单取消次数',
                dt string COMMENT 'dt'
                  )

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
create table IF NOT EXISTS """+dbName+""".app_dm_ae_rfm_user"""+extendname+"""_tb( 
        user_id string COMMENT 'user id',  
        latest_trade_date date COMMENT '最近一次消费时间',  
        trade_count int COMMENT '累计消费次数',  
        trade_money_summary double COMMENT '累计消费金额',  
        order_qtty bigint COMMENT '累计消费件数',  
        channel string COMMENT '下单渠道',  
        first_trade_date date COMMENT '第一次消费时间',  
        sku string COMMENT 'sku',  
        brand_favor string COMMENT '品牌偏好',  
        cate_favor string COMMENT '品类偏好',  
        cancel_times int COMMENT '订单取消次数'
    )
PARTITIONED BY (dt string)
STORED AS ORC;
"""
else:
    create_sql="""create table IF NOT EXISTS """+dbName+""".tmp_dm_ae_rfm_deal"""+extendname+"""_tb( 
            user_id string COMMENT 'user id', 
            order_id string COMMENT 'order_id', 
            trade_date date COMMENT '订单时间', 
            trade_money double COMMENT '消费金额', 
            sale_qtty double COMMENT '消费件数', 
            channel string COMMENT '下单渠道', 
            sku string COMMENT 'sku', 
            brand string COMMENT '品牌', 
            cate string COMMENT '品类', 
            cancel_flag string COMMENT '订单取消标记',
            dt string COMMENT 'dt'
            )

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
create table IF NOT EXISTS """+dbName+""".app_dm_ae_rfm_deal"""+extendname+"""_tb( 
            user_id string COMMENT 'user id', 
            order_id string COMMENT 'order_id', 
            trade_date date COMMENT '订单时间', 
            trade_money double COMMENT '消费金额', 
            sale_qtty double COMMENT '消费件数', 
            channel string COMMENT '下单渠道', 
            sku string COMMENT 'sku', 
            brand string COMMENT '品牌', 
            cate string COMMENT '品类', 
            cancel_flag string COMMENT '订单取消标记'
    )
PARTITIONED BY (dt string)
STORED AS ORC;
"""
##创建表
# cmd_create = 'hive -e """'+create_sql.replace('"', "\'")+'"""'
# print(cmd_create)
# p = subprocess.Popen(cmd_create, shell=True, stdout=subprocess.PIPE)


cmd = 'hive -e """'+sqlCond.replace('"', "\'")+'"""'
print(cmd)
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
# date range
def dateRange(start, end, step=1, format="%Y-%m-%d"):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days+1
    return [strftime(strptime(start, format) + datetime.timedelta(i), format) for i in range(0, days, step)]
#latest_trade_date
def latest_trade_date(end_day):
    format="%Y-%m-%d"
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    return strftime(strptime(end_day, format) - datetime.timedelta(random.randint(0,280)), format)
#first_trade_date
def first_trade_date():
    format="%Y-%m-%d"
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    return strftime(strptime("2019-03-01", format) - datetime.timedelta(random.randint(0,600)), format)
# trade_count
def trade_count():
    return str(random.randint(3, 30))
# trade_money_summary
def trade_money_summary():
    return str(random.uniform(100, 30000))
# order_qtty 必须大于trade_count
def order_qtty(trade_cou):
    return str(random.randint(trade_cou, 200))
# order_qtty 必须大于trade_count
def sku_cnt(qtty):
    return str(random.randint(2, qtty))
# channel
def channel():
    items = ['iphone', 'mobile_phone', 'android', 'ipad', 'win_mobile', 'm', 'wx', 'qq']
    return random.choice(items)

# sku
def sku():
    return ''.join(str(random.choice(range(10))) for _ in range(8))
# brand preference
def brandPreference():
    items=['帮宝适#苹果', '小米', '海飞丝#3M#佳洁士', '蔻驰#爱马仕#劳力士', '华为#vivo#一加#oppo#荣耀']
    return random.choice(items)
# cate preference
def catePreference():
    items=['数码#家电', '家具', '家居#3c#数码', '美妆#生鲜#手机', '日用#硬件#健康#美妆#小家电']
    return random.choice(items)

# cancel
def cancelCount(ord_cnt):
    return str(random.randint(0, ord_cnt))
    # cancel
def cancelflag():
    return str(random.randint(0, 2))
# order
def order():
    result = random.randint(0, 2)
    head = str(random.randint(1,8))
    if result == 1:
        return head + ''.join(str(random.choice(range(10))) for _ in range(8))
    if result == 0:
        return head + ''.join(str(random.choice(range(10))) for _ in range(9))
    return head + ''.join(str(random.choice(range(10))) for _ in range(10))
# brand
def brand():
    items = ['4300', '17191', '76933', '151345', '153787', '185334', '179879', '378049', '806', '201655', '177565', '38585']
    return random.choice(items)
# category
def category():
    items = ['9987', '1672', '652', '670', '5025', '1315', '1319', '1713', '6728', '11729', '4053', '737']
    return random.choice(items)
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
    return random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) + random.choice(enum) + \
           random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) + random.choice(enum) + random.choice(num_enum) \
           + random.choice(enum) + random.choice(num_enum)
# md5 phone
def md5Phone():
    second = [3, 4, 5, 7, 8][random.randint(0, 4)]

    third = {
        3: random.randint(0, 9),
        4: [5, 7, 9][random.randint(0, 2)],
        5: [i for i in range(10) if i != 4][random.randint(0, 8)],
        7: [i for i in range(10) if i not in [4, 9]][random.randint(0, 7)],
        8: random.randint(0, 9),
    }[second]

    suffix = random.randint(9999999,100000000)

    phone = "1{}{}{}".format(second, third, suffix)
    hl = hashlib.md5()
    hl.update(phone.encode(encoding='utf-8'))

    return hl.hexdigest()

f = open(fileName, 'w')
totalSize = 0
progress = 0

sizeOfEachDate = cnt

fieldDelimiter = ','

print("Start to Create Data ...")

dateList = dateRange(start_day, end_day)
print(dateList)
def userData(dateList):
    for dateValue in dateList:
        print("dateValue:" + dateValue + "\n")
        rowLoop = 0
        sizeOfEachDate=cnt;
        while rowLoop < sizeOfEachDate:
            randValue = random.randint(1,50)
            if randValue > 5:
                order_cnt = trade_count()
                qtty = order_qtty(int(order_cnt))
                cancel_times = cancelCount(int(order_cnt))
                userIdstr = userId(rowLoop)
                # userIdstr = "userid_" + str(rowLoop)
                lineText = userIdstr + fieldDelimiter + latest_trade_date(dateValue) + fieldDelimiter + order_cnt \
                           + fieldDelimiter + trade_money_summary() + fieldDelimiter + qtty + fieldDelimiter + channel() + fieldDelimiter + first_trade_date() \
                           + fieldDelimiter +sku_cnt(int(qtty)) + fieldDelimiter + brandPreference() + fieldDelimiter + catePreference() + fieldDelimiter +cancel_times + fieldDelimiter + dateValue
                lineText = lineText.strip()
                lineText += '\n'
                f.write(lineText)
            rowLoop += 1
    f.close()
    sqlinsert="""LOAD DATA LOCAL INPATH '"""+filePath+"""' OVERWRITE INTO TABLE """+dbName+""".tmp_dm_ae_rfm_user"""+extendname+"""_tb;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set mapreduce.job.reduces=5;
    set hive.merge.mapfiles = true;
    set hive.merge.mapredfiles = true;
    set hive.merge.size.per.task = 256000000;
    set hive.merge.smallfiles.avgsize=128000000;
    set hive.merge.orcfile.stripe.level=false;
    INSERT OVERWRITE TABLE """+dbName+""".rfm_user_data"""+extendname+"""_demo partition
	(dt
	)
    SELECT
            user_id,
            latest_trade_date,
            trade_count,
            trade_money_summary,
            order_qtty,
            channel,
            first_trade_date,
            sku,
            brand_favor,
            cate_favor,
            cancel_times,
            dt
            
    FROM """+dbName+""".tmp_dm_ae_rfm_user"""+extendname+"""_tb where dt >='"""+start_day+"""' and dt<='"""+end_day+"""';"""
    print(sqlinsert)
    ht.exec_sql(schema_name = dbName, table_name = 'rfm_user_data'+extendname+'_demo',
                sql = sqlinsert, merge_flag = True,merge_part_dir =['dt=' + dateList[0]], merge_type='mr')
def orderDataHive(dateList):
    for dateValue in dateList:
        print("dateValue:" + dateValue + "\n")
        rowLoop = 0
        sizeOfEachDate=cnt
        while rowLoop < sizeOfEachDate:
            userIdstr = userId(rowLoop)
            randValue = random.randint(1,50)
            if randValue > 20:
                # userIdstr = "userid_" + str(rowLoop)
                lineText = userIdstr + fieldDelimiter + order() + fieldDelimiter +  dateValue + fieldDelimiter + trade_money_summary() + fieldDelimiter + order_qtty(3) \
                           +fieldDelimiter + channel()  +fieldDelimiter + sku() + fieldDelimiter \
                           + brand() + fieldDelimiter + category() + fieldDelimiter + cancelflag() + fieldDelimiter + dateValue
                lineText = lineText.strip()
                lineText += '\n'
                f.write(lineText)
            rowLoop += 1
    sqlinsert="""LOAD DATA LOCAL INPATH '"""+filePath+"""' OVERWRITE INTO TABLE """+dbName+""".tmp_dm_ae_rfm_deal"""+extendname+"""_tb;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set mapreduce.job.reduces=5;
    set hive.merge.mapfiles = true;
    set hive.merge.mapredfiles = true;
    set hive.merge.size.per.task = 256000000;
    set hive.merge.smallfiles.avgsize=128000000;
    set hive.merge.orcfile.stripe.level=false;
    INSERT OVERWRITE TABLE """+dbName+""".rfm_deal_data"""+extendname+"""_demo partition
	(dt)
    SELECT
    	user_id         ,
    	order_id        ,
    	trade_date,
    	trade_money     ,
    	sale_qtty       ,
    	channel         ,
    	sku             ,
    	brand           ,
    	cate            ,
    	cancel_flag,
    	dt
    FROM
    	"""+dbName+""".tmp_dm_ae_rfm_deal"""+extendname+"""_tb
    where dt >='"""+start_day+"""' and dt<='"""+end_day+"""';"""
    print(sqlinsert)
    ht.exec_sql(schema_name = dbName, table_name = 'rfm_user_data'+extendname + '_demo',
                sql = sqlinsert, merge_flag = True,merge_part_dir =['dt=' + dateList[0]], merge_type='mr')
if(sys.argv[3]=='user'):
    userData(dateList)
else:
    orderDataHive(dateList)
f.close()
print("Complete.")