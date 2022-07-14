package com.jd.easy.audience.task.transfer.jmq.conf;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * 环境配置信息
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class EnvConf {
    /**
     * 生成flink流式处理执行环境
     *
     * @return StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用系统当前时间作为每一条数据的处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        CheckpointConfig config = env.getCheckpointConfig();
//        //设置checkpoint间隔和模式
//        config.setCheckpointInterval(60000);
//        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        config.setMinPauseBetweenCheckpoints(5000);
//        //设置checkpoint超时时间
//        config.setCheckpointTimeout(120000);
//        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //设置任务失败重启策略, 每隔3分钟重启一次, 最多可以尝试10次
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.minutes(3)));
//        initGlobalParam(env);
        //设置checkpoint间隔和模式
        env.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    /**
     * 流处理执行环境，设置全局参数ø
     *
     * @param env 执行环境
     */
    public static void initGlobalParam(StreamExecutionEnvironment env) {
        Map<String, String> data = new HashMap<>(4);
        data.put("ts", System.currentTimeMillis() + "");
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(data));
    }

}
