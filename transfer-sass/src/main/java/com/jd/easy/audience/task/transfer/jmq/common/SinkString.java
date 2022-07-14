package com.jd.easy.audience.task.transfer.jmq.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author cdxiongmei
 * @version V1.0
 */
@Slf4j
public class SinkString implements SinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        log.info("sink print:" + value);
    }

}
