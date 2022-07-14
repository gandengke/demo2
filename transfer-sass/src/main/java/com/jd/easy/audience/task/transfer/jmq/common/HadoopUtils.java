package com.jd.easy.audience.task.transfer.jmq.common;

import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * hadoop 功能类
 *
 * @author cdxuqiang3
 * @date 2021/8/12
 */
public class HadoopUtils {

    public static final String BDP_DIR = "hadoop_conf";
    public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    public static final String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";

    public static Configuration getHadoopConf(String bdpUser) {
        Map<String, String> envParam = System.getenv();
        String hadoopConfDir = envParam.getOrDefault("HADOOP_CONF_DIR", BDP_DIR);
        Configuration conf = new Configuration();
        if (BDP_DIR.equals(hadoopConfDir)) {
            conf.addResource(hadoopConfDir + "/core-site.xml");
            conf.addResource(hadoopConfDir + "/hdfs-site.xml");
        } else {
            conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
            conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
        }
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set(HADOOP_USER_NAME, bdpUser);
        conf.set(HADOOP_PROXY_USER, bdpUser);
        setEnv(HADOOP_USER_NAME, bdpUser);
        setEnv(HADOOP_PROXY_USER, bdpUser);
        return conf;
    }

    public static void setEnv(String key, String value) {
        try {
            System.setProperty(key, value);
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object mapObject = field.get(env);
            if (mapObject instanceof Map) {
                Map<String, String> writableEnv = (Map) mapObject;
                writableEnv.put(key, value);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    public static org.apache.flink.configuration.Configuration getFlinkFsConf(String bdpUser) {
        Configuration conf = getHadoopConf(bdpUser);
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();

        for (Map.Entry<String, String> entry : conf) {
            configuration.setString(entry.getKey(), entry.getValue());
        }
        return configuration;
    }

    public static FileSystem getHadoopFs(String bdpUser) throws IOException {
        return FileSystem.get(getHadoopConf(bdpUser));
    }

    public static org.apache.flink.core.fs.FileSystem getFlinkFs(String bdpUser) throws IOException {
        return new HadoopFileSystem(getHadoopFs(bdpUser));
    }

}
