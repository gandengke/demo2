package com.jd.easy.audience.task.transfer.jmq.bucketer;

import com.alibaba.fastjson.JSONObject;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 按格式生成hdfs目录
 *
 * @author cdxuqiang3
 * @date 2021/8/9
 */
@Slf4j
public class DayHourPathTenantBucketer extends BasePathBucketer<String> {

    /**
     * 间隔15分钟处理一次数据
     */
    private static final int INTERVAL = 15;
    @Setter
    private String subDir = "";

    @Override
    public Path getBucketPath(Clock clock, Path basePath, String element) {
        LocalDateTime now = LocalDateTime.now();
        int minDir = (now.getMinute() / INTERVAL) * INTERVAL;
        if (StringUtils.isNotBlank(this.subDir) && JSONObject.isValidObject(element)) {
            String subDirDes = "";
            JSONObject eleJson = JSONObject.parseObject(element);
            subDirDes = eleJson.getString(this.subDir);
            log.info("DayHourDbPathBucketer json={},keys={}, subDir={}", eleJson.toString(), eleJson.keySet().toArray().toString(), subDirDes);
            return new Path(basePath + File.separator + subDirDes  + File.separator + now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd/HH")) + "/" + minDir);
        }
        return new Path(basePath + File.separator + now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd/HH")) + "/" + minDir);
    }

    public DayHourPathTenantBucketer(String subDir) {
        this.subDir = subDir;
    }
}
