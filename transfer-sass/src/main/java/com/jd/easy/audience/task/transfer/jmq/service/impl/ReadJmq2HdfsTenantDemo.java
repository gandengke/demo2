package com.jd.easy.audience.task.transfer.jmq.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.jd.bdp.rrd.apus.flink.sdk.batchsink2.BatchSinkConfig;
import com.jd.easy.audience.task.transfer.jmq.batchsink.AbstractBatchBucketSinkFunction;
import com.jd.easy.audience.task.transfer.jmq.bucketer.DayHourPathTenantBucketer;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 根据租户进行数据的落库
 *
 * @author cdxiongmei
 * @version V1.0
 */
@Slf4j
public class ReadJmq2HdfsTenantDemo implements MySinkInterface {
    private static final char FIELD_SEPTOR = '\u0002';

    @Override
    public void doJob(FlinkJobConfig config) throws Exception {
        //处理json数据
        StreamExecutionEnvironment env = EnvConf.getEnv();
        env.setParallelism(4);
        Properties props = initConfig(config.getAppId());
        FlinkKafkaConsumer<String> consumerInstance = new FlinkKafkaConsumer(config.getTopicName(), new SimpleStringSchema(), props);
        consumerInstance.setCommitOffsetsOnCheckpoints(true);
        SingleOutputStreamOperator<String> source = env.addSource(consumerInstance)
                .name("apus-source"); //算子名称
        //batch bucketing
        AbstractBatchBucketSinkFunction sinkNew = new AbstractBatchBucketSinkFunction(BatchSinkConfig.builder()
                .batchProcessorThreadNum(2)
                .batchSize(3)
                .batchLingerMs(1000 * 40)
                .build(), ConfigProperties.getHdfsPath(config.getAccountName()) + "/" + config.getTopicName(), HadoopUtils.getFlinkFsConf(config.getAccountName()));
        sinkNew.setBucketer(new DayHourPathTenantBucketer(config.getExtendPathField()));
        if (StringUtils.isNotBlank(config.getMsgType()) && "text".equalsIgnoreCase(config.getMsgType())) {
            //数据流
            SingleOutputStreamOperator<String> sourceStream = source.map((org.apache.flink.api.common.functions.MapFunction<String, String>) s -> {
                return strToJson(s, config.getFieldsSet(), FIELD_SEPTOR);
            })
                    .uid("source_map")
                    .filter(s -> s != null).uid("source_filter");
            sourceStream.addSink(sinkNew).name("stream_hdfs_sink").setParallelism(8);
        } else if (StringUtils.isNotBlank(config.getMsgType()) && "logbook".equalsIgnoreCase(config.getMsgType())) {
            //logbook
            SingleOutputStreamOperator<String> sourceStream = source.map((org.apache.flink.api.common.functions.MapFunction<String, String>) s -> {
                if (!JSONObject.isValidObject(s)) {
                    log.info(s + " is invalid jsonobject");
                    return null;
                } else {
                    String msg = JSONObject.parseObject(s).getString("msg");
                    return strToJson(msg, config.getFieldsSet(), FIELD_SEPTOR);
                }
            })
                    .uid("source_map")
                    .filter(s -> s != null).uid("source_filter");
            sourceStream.addSink(sinkNew).name("stream_hdfs_sink").setParallelism(2);

        } else {
            //json格式
            source.addSink(sinkNew)
                    .name("json_hdfs_sink").setParallelism(8);
        }
        env.execute("JMQ2HDFS_FLINK_TASK");
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

    public static String strToJson(String str, String fields, char fieldSeptor) {
        StringBuilder sb = new StringBuilder();
        String[] dataArr = str.split(fieldSeptor + "");
        String[] fieldArr = fields.split(",");
        if (dataArr.length != fieldArr.length) {
            log.info("the length of source is not equals to the rule str={}", str);
            return sb.toString();
        }
        sb.append("{");
        for (int i = 0; i < fieldArr.length; i++) {
            sb.append("\"" + fieldArr[i] + "\"");
            sb.append(":");
            sb.append("\"" + dataArr[i] + "\"");
            sb.append(",");
        }
        return sb.toString().substring(0, sb.length() - 1) + "}";
    }
}
