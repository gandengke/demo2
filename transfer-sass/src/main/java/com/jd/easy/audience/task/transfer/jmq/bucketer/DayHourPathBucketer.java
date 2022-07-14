package com.jd.easy.audience.task.transfer.jmq.bucketer;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 按格式生成hdfs目录
 *
 * @author cdxuqiang3
 * @date 2021/8/9
 */
public class DayHourPathBucketer implements Bucketer<String> {

    /**
     * 间隔15分钟处理一次数据
     */
    private static final int INTERVAL = 15;

    @Override
    public Path getBucketPath(Clock clock, Path basePath, String element) {
        LocalDateTime now = LocalDateTime.now();
        return new Path(basePath + "/" + now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd/HH")) + "/" + (now.getMinute() / INTERVAL));
    }
}
