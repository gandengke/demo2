package com.jd.easy.audience.task.transfer.jmq.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.jd.easy.audience.task.transfer.jmq.bucketer.DayHourPathBucketer;
import com.jd.easy.audience.task.transfer.jmq.common.FlinkJobConfig;
import com.jd.easy.audience.task.transfer.jmq.common.HadoopUtils;
import com.jd.easy.audience.task.transfer.jmq.conf.EnvConf;
import com.jd.easy.audience.task.transfer.jmq.service.MySinkInterface;
import com.jd.easy.audience.task.transfer.property.ConfigProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author cdxiongmei
 * @version V1.0
 */
@Slf4j
public class ReadJmq2HdfsDemo2 implements MySinkInterface {
    private static final char FIELD_SEPTOR = '\u0002';

    @Override
    public void doJob(FlinkJobConfig config) throws Exception {
        //处理json数据
        StreamExecutionEnvironment env = EnvConf.getEnv();
        Properties props = initConfig(config.getAppId());
        FlinkKafkaConsumer<String> consumerInstance = new FlinkKafkaConsumer(config.getTopicName(), new SimpleStringSchema(), props);
        consumerInstance.setCommitOffsetsOnCheckpoints(true);
        SingleOutputStreamOperator<String> source = env.addSource(consumerInstance)
                .name("apus-source"); //算子名称
        BucketingSink<String> sink = new BucketingSink<>(ConfigProperties.getHdfsPath(config.getAccountName()) + "/" + config.getTopicName());
        sink.setUseTruncate(false);
        sink.setFSConfig(HadoopUtils.getFlinkFsConf(config.getAccountName()));
        sink.setBucketer(new DayHourPathBucketer());
        sink.setBatchSize(1024 * 1024 * 100); //100m文件尺寸
        if (StringUtils.isNotBlank(config.getMsgType()) && "text".equalsIgnoreCase(config.getMsgType())) {
            //数据流
            SingleOutputStreamOperator<String> sourceStream = source.map((org.apache.flink.api.common.functions.MapFunction<String, String>) s -> {
                log.info("text old string:{}", s);
                log.info("text new string:{}", s.replace(FIELD_SEPTOR, ','));
                return s;
            })
                    .uid("source_map")
                    .filter(s -> s != null).uid("source_filter");
            sourceStream.addSink(sink).name("stream_hdfs_sink").setParallelism(8);

        } else if (StringUtils.isNotBlank(config.getMsgType()) && "logbook".equalsIgnoreCase(config.getMsgType())) {
            //logbook
            SingleOutputStreamOperator<String> sourceStream = source.map((org.apache.flink.api.common.functions.MapFunction<String, String>) s -> {
                log.info("logbook old string:{}", s);
                if (!JSONObject.isValidObject(s)) {
                    log.info(s + " is invalid jsonobject");
                    return null;
                } else {
                    log.info(s + " is valid jsonobject");
//                    String msg = JSONObject.parseObject(s).getString("msg").replace(FIELD_SEPTOR, ',');
                    String msg = JSONObject.parseObject(s).getString("msg");
                    log.info("msg info {}", msg);
                    return msg;
                }
            })
                    .uid("source_map")
                    .filter(s -> s != null).uid("source_filter");
//            sink.setWriter(new PmpMonitorWriter(fieldNames.split(","), ","));
            sourceStream.addSink(sink).name("stream_hdfs_sink").setParallelism(8);

        } else {
            source.addSink(sink)
                    .name("json_hdfs_sink").setParallelism(8);
        }
//        source.addSink(sink)
//                .name("json_hdfs_sink").setParallelism(8);
        env.execute();
    }

    private Properties initConfig(String appId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigProperties.ADDRESS);

        // 需指定group.id，值是管理端的app
        props.put(ConsumerConfig.GROUP_ID_CONFIG, appId);

        // 需指定client.id，值是管理端的app
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, appId);

        // 其他配置根据实际情况配置，这里只是演示
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }
}
