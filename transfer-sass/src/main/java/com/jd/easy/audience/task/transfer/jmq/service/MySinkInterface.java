package com.jd.easy.audience.task.transfer.jmq.service;

import com.jd.easy.audience.task.transfer.jmq.common.FlinkJobConfig;

/**
 * 处理
 *
 * @author cdxiongmei
 * @version V1.0
 */
public interface MySinkInterface {
    void doJob(FlinkJobConfig config) throws Exception;
}
