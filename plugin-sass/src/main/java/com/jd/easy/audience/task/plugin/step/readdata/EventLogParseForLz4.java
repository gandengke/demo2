package com.jd.easy.audience.task.plugin.step.readdata;

import com.alibaba.fastjson.JSONObject;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class EventLogParseForLz4 {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EventLogParseForLz4.class);

    /**
     * @throws
     * @title main
     * @description 获取任务的算力数据
     * @author cdxiongmei
     * @param: args
     * @date 2021/8/10 下午8:10
     */
    public static void main(String[] args) throws Exception {
        String timeStr = args[0];
        String[] appIds = args[1].split(",");
        LOGGER.info("application time" + args[0]);
        LOGGER.info("application len" + appIds.length);
        Configuration conf = new Configuration();

        for (String appid : appIds) {
            String hdfsPath = new StringBuilder("hdfs://ns1/user/spark/log/").append(timeStr).append(File.separator).append(appid).append(".lz4").toString();
            LOGGER.info("metrics start hdfsPath={}", hdfsPath);
            FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
            FSDataInputStream in = fs.open(new Path(hdfsPath));
            LZ4BlockInputStream bis = new LZ4BlockInputStream(in);
            try {
                sparkAppMetrics(bis, hdfsPath);
            } catch (Exception ex) {
                LOGGER.info("metrics Exception hdfsPath={}", hdfsPath);
                LOGGER.info(String.valueOf(ex));
            } finally {
                bis.close();
                in.close();
                fs.close();
            }
        }
    }

    /**
     * @throws
     * @title sparkAppMetrics
     * @description 度量指标获取
     * @author cdxiongmei
     * @param: bis
     * @param: hdfsPath
     * @updateTime 2021/8/10 下午8:10
     */
    public static void sparkAppMetrics(LZ4BlockInputStream bis, String hdfsPath) throws Exception {
        String strline = null;
        Map<String, Long> timelist = new HashMap<>(16);
        long appstart = 0;
        long append = 0;
        Map<String, Double> executorusememoryMetrics = new HashMap<>(16);
        Map<String, Double> executorusememoryMetricsJVM = new HashMap<>(16);
        Map<String, Double> executorUseMemoryMetricsOff = new HashMap<>(16);
        Map<String, Double> executorUseMemoryMetricsJvmOff = new HashMap<>(16);
        double driverUseMemoryMetrics = 0.0;
        double driverUseMemoryMetricsJvm = 0.0;
        double driverUseMemoryMetricsOff = 0.0;
        double driverUseMemoryMetricsJvmOff = 0.0;

        double driverwant = 0;
        double executorwant = 0;
        double executorwantOff = 0;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(bis));
        while ((strline = bufferedReader.readLine()) != null) {
            JSONObject json = JSONObject.parseObject(strline.trim());
//                System.out.println(strline);
            if (json.get("Event").toString().equalsIgnoreCase("SparkListenerApplicationStart")) {
                appstart = Long.parseLong(json.get("Timestamp").toString());
            } else if (json.get("Event").toString().equalsIgnoreCase("SparkListenerExecutorAdded")) {
                timelist.put(json.get("Executor ID").toString(), Long.parseLong(json.get("Timestamp").toString()));
                LOGGER.info(new StringBuilder(json.get("Executor ID").toString()).append(",").append(json.get("Timestamp").toString()).toString());
            } else if (json.get("Event").toString().equalsIgnoreCase("SparkListenerApplicationEnd")) {
                append = Long.parseLong(json.get("Timestamp").toString());
            } else if (json.get("Event").toString().equalsIgnoreCase("SparkListenerEnvironmentUpdate")) {
                //申请使用的内存数据
                JSONObject obj = JSONObject.parseObject(json.get("Spark Properties").toString());
                driverwant = Double.parseDouble(obj.get("spark.driver.memory").toString().replaceAll("G|g", "")) * 1024; //mb
//                    double overHead = 0.0;
                if (obj.get("spark.executor.memoryOverhead") != null) {
                    //offheap memory
                    executorwantOff = Double.parseDouble(obj.get("spark.executor.memoryOverhead").toString());
                }
                executorwant = Double.parseDouble(obj.get("spark.executor.memory").toString().replaceAll("G|g", "")) * 1024; //mb

            } else if (json.get("Event").toString().equalsIgnoreCase("SparkListenerStageExecutorMetrics") && json.get("Executor ID").toString().equalsIgnoreCase("driver")) {

                getDriverMetric(json, driverUseMemoryMetrics, driverUseMemoryMetricsJvm, driverUseMemoryMetricsOff, driverUseMemoryMetricsJvmOff);
            } else if (json.get("Event").toString().equalsIgnoreCase("SparkListenerStageExecutorMetrics")) {
                getExcutorMetric(json, executorusememoryMetrics, executorusememoryMetricsJVM, executorUseMemoryMetricsOff, executorUseMemoryMetricsJvmOff);
            }
        }
        LOGGER.info("============BaseInfo start============");
        LOGGER.info("hdfsPath={},appstart={},append={},timelist={}",
                hdfsPath,
                appstart,
                append,
                timelist.toString());
        LOGGER.info("============BaseInfo end============");
        double executorUseSumMetrics = 0.0;
        double executorUseSumMetricsOff = 0.0;
        double executorUseSumMetricsJvm = 0.0;
        double executorUseSumMetricsJvmOff = 0.0;
        for (String excutor : executorusememoryMetrics.keySet()) {
            executorUseSumMetrics = executorUseSumMetrics + executorusememoryMetrics.get(excutor);
        }
        for (String excutor : executorUseMemoryMetricsOff.keySet()) {
            executorUseSumMetricsOff = executorUseSumMetricsOff + executorUseMemoryMetricsOff.get(excutor);
        }
        for (String excutor : executorusememoryMetricsJVM.keySet()) {
            executorUseSumMetricsJvm = executorUseSumMetricsJvm + executorusememoryMetricsJVM.get(excutor);
        }
        for (String excutor : executorUseMemoryMetricsJvmOff.keySet()) {
            executorUseSumMetricsJvmOff = executorUseSumMetricsJvmOff + executorUseMemoryMetricsJvmOff.get(excutor);
        }
        bufferedReader.close();
    }

    /**
     * @throws
     * @title getDriverMetric
     * @description 获取驱动数据统计
     * @author cdxiongmei
     * @param: json
     * @param: driverUseMemoryMetrics
     * @param: driverUseMemoryMetricsJvm
     * @param: driverUseMemoryMetricsOff
     * @param: driverUseMemoryMetricsJvmOff
     * @updateTime 2021/8/11 上午9:42
     */
    private static void getDriverMetric(JSONObject json, double driverUseMemoryMetrics, double driverUseMemoryMetricsJvm, double driverUseMemoryMetricsOff, double driverUseMemoryMetricsJvmOff) {
        //按照stage维度，可能有重复的excutor，需要汇总
        LOGGER.info("================SparkListenerStageExecutorMetrics===============");
        LOGGER.info(json.toString());
        JSONObject obj = JSONObject.parseObject(json.get("Executor Metrics").toString());
        double heapmemory = Double.parseDouble(obj.get("OnHeapUnifiedMemory").toString()) / 1024 / 1024; //存储内存和执行内存
        double offheapmemory = driverUseMemoryMetrics + Double.parseDouble(obj.get("OffHeapUnifiedMemory").toString()) / 1024 / 1024;
        double heapmemoryJVM = Double.parseDouble(obj.get("JVMHeapMemory").toString()) / 1024 / 1024; //jvm内存
        double offheapmemoryJVM = driverUseMemoryMetrics + Double.parseDouble(obj.get("JVMOffHeapMemory").toString()) / 1024 / 1024;
        LOGGER.info("driveruseMetrics:heap={},offheap={}", heapmemory, offheapmemory);
        LOGGER.info("driveruseMetrics_jvm:heap_jvm={},offheap_jvm={}", heapmemoryJVM, offheapmemoryJVM);
        driverUseMemoryMetrics = driverUseMemoryMetrics + heapmemory;
        driverUseMemoryMetricsJvm = driverUseMemoryMetricsJvm + heapmemoryJVM;
        driverUseMemoryMetricsOff = driverUseMemoryMetricsOff + offheapmemory;
        driverUseMemoryMetricsJvmOff = driverUseMemoryMetricsJvmOff + offheapmemoryJVM;
    }

    /**
     * @throws
     * @title getExcutorMetric
     * @description 获取excutor算力统计
     * @author cdxiongmei
     * @param: json
     * @param: executorusememoryMetrics
     * @param: executorusememoryMetricsJVM
     * @param: executorUseMemoryMetricsOff
     * @param: executorUseMemoryMetricsJvmOff
     * @updateTime 2021/8/11 上午9:43
     */
    private static void getExcutorMetric(JSONObject json, Map<String, Double> executorusememoryMetrics, Map<String, Double> executorusememoryMetricsJVM, Map<String, Double> executorUseMemoryMetricsOff, Map<String, Double> executorUseMemoryMetricsJvmOff) {
        //按照stage维度，可能有重复的excutor，需要汇总
        LOGGER.info("================SparkListenerStageExecutorMetrics===============");
        LOGGER.info(json.toString());
        JSONObject obj = JSONObject.parseObject(json.get("Executor Metrics").toString());
        double heapmemory = Double.parseDouble(obj.get("OnHeapUnifiedMemory").toString()) / 1024 / 1024; //存储内存和执行内存
//        double offheapmemory = driverUseMemoryMetrics + Double.parseDouble(obj.get("OffHeapUnifiedMemory").toString()) / 1024 / 1024;
        double offheapmemory = Double.parseDouble(obj.get("OffHeapUnifiedMemory").toString()) / 1024 / 1024;
        double heapmemoryJVM = Double.parseDouble(obj.get("JVMHeapMemory").toString()) / 1024 / 1024; //jvm内存
//        double offheapmemoryJVM = driverUseMemoryMetrics + Double.parseDouble(obj.get("JVMOffHeapMemory").toString()) / 1024 / 1024;
        double offheapmemoryJVM = Double.parseDouble(obj.get("JVMOffHeapMemory").toString()) / 1024 / 1024;

        String executorId = json.get("Executor ID").toString();
        if (executorusememoryMetrics.containsKey(executorId)) {
            executorusememoryMetrics.put(executorId, heapmemory + executorusememoryMetrics.get(executorId));
            executorusememoryMetricsJVM.put(executorId, heapmemoryJVM + executorusememoryMetricsJVM.get(executorId));
            executorUseMemoryMetricsOff.put(executorId, offheapmemory + executorUseMemoryMetricsOff.get(executorId));
            executorUseMemoryMetricsJvmOff.put(executorId, offheapmemoryJVM + executorUseMemoryMetricsJvmOff.get(executorId));
        } else {
            executorusememoryMetrics.put(executorId, heapmemory);
            executorUseMemoryMetricsOff.put(executorId, offheapmemory);
            executorusememoryMetricsJVM.put(executorId, heapmemoryJVM);
            executorUseMemoryMetricsJvmOff.put(executorId, offheapmemoryJVM);
        }
    }
}
