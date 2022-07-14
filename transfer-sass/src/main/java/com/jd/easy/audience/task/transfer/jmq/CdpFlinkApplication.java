package com.jd.easy.audience.task.transfer.jmq;
import com.jd.easy.audience.task.transfer.jmq.common.FlinkJobConfig;
import com.jd.easy.audience.task.transfer.jmq.service.MySinkInterface;

import java.io.IOException;
import java.util.Arrays;

/**
 * 读取字符串
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class CdpFlinkApplication {
    public static void main(String[] args) throws Exception {
        // 读取配置

        if (args.length != 7) {
            throw new IOException("The length of args  is less than 5.");
        }

        String className = Arrays.stream(args).findFirst().get();
        String appId = args[1];
        String topicName = args[2];
        String msgType = args[3];
        String accountName = args[4];
        String extendPathField = args[5];
        String fieldsList = args[6];
        FlinkJobConfig config = new FlinkJobConfig();
        config.setAppId(appId);
        config.setTopicName(topicName);
        config.setMsgType(msgType);
        config.setAccountName(accountName);
        config.setExtendPathField(extendPathField);
        config.setFieldsSet(fieldsList);
        System.out.println("config:" + config.toString());
        System.out.println("HADOOP_USER_NAME:" + System.getProperty("HADOOP_USER_NAME"));
        System.setProperty("HADOOP_USER_NAME", accountName);
        System.out.println("HADOOP_USER_NAME:" + System.getProperty("HADOOP_USER_NAME"));
        System.out.println("classname:" + className);
        Class c = Class.forName(className); //baoming 是该类所在的包   leiming 是一个该类的名称的Stringcom.jd.easy.audience.task.transfer.jmq.JnosFlinkApplication
        MySinkInterface flag = (MySinkInterface) c.newInstance();
        flag.doJob(config);

    }

}
