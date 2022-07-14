package com.jd.easy.audience.task.plugin.run.spark;

import com.jd.easy.audience.common.oss.OssClientTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.jss.Credential;
import com.jd.jss.JingdongStorageService;
import com.jd.jss.client.ClientConfig;
import com.jd.jss.client.StorageHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * test
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class WriteOssDemo {
    public static void main(String[] args) throws IOException {
        String endpoint = args[0];
        String accessKey = args[1];
        String secretKey = args[2];
        String bucket = args[3];
        //easy-audience/10934/dataset/label/11695
        //pre-cdp/10042/data/qingteng2/result/20220701/1656573392903119
        String ossPath = args[4];
        String fileFormat = args[5];
        System.out.println("endPoint:" + endpoint + ", accessKey:" + accessKey + ", outputpath:" + secretKey);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSessionWithOssTest("TestReadStep", endpoint, accessKey, secretKey, OssClientTypeEnum.JFS);

        String jfsPath = JdCloudOssUtil.buildsOssPath(bucket, ossPath, OssClientTypeEnum.JFS);
        System.out.println("jfsPath:" + jfsPath);
        Dataset<Row> dataOld = sparkSession.read().option("header", "true").format(fileFormat).load(jfsPath);
        System.out.println(dataOld.count());
    }

    private static JingdongStorageService createjss(String endPoint, String accessKey, String secretKey) {
        JingdongStorageService jss = null;
        if (!StringUtils.isAnyBlank(endPoint, accessKey, secretKey)) {
            Credential credential = new Credential(accessKey, secretKey);
            ClientConfig config = new ClientConfig();
            config.setEndpoint(endPoint);
            config.setSocketTimeout(6000000);
            config.setConnectionTimeout(6000000);
            jss = new JingdongStorageService(credential, config);
        }
        return jss;
    }

    private static StorageHttpClient getClient(String endPoint, String accessKey, String secretKey) {
        Credential credential = new Credential(accessKey, secretKey);
        ClientConfig config = new ClientConfig();
        config.setEndpoint(endPoint);
        config.setSocketTimeout(6000000);
        config.setConnectionTimeout(6000000);
        //只有AE本地的OSS需要创建client，用于分片上传
        return new StorageHttpClient(config, credential);
    }

    public static StructType initSchema() {
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("labelName", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("labelValue", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("account_id", DataTypes.StringType, false));
        StructType struct = DataTypes.createStructType(fieldList);
        return struct;
    }

    static Long getModificationTime(Path path, FileSystem fileSystem) {
        try {
            FileStatus[] instatus = fileSystem.listStatus(path);
            Long fileSize = 0L;
            for (FileStatus fileEle :
                    instatus) {
                if (fileEle.isDirectory()) {
                    Long sizeTmp = getModificationTime(fileEle.getPath(), fileSystem);
                    fileSize = fileSize + sizeTmp;
                } else {
                    fileSize = fileSize + fileEle.getLen();
                }
            }
            return fileSize;
        } catch (Exception ex) {
            System.out.println("获取表信息有异常" + ex);
            return 0L;
        }
    }

    public static void cleanHdfsFile(String filePath, Configuration conf) {
        if (filePath == null || conf == null) {
            return;
        }
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);
            fileSystem.delete(new Path(filePath), true);
        } catch (IOException e) {
            System.out.println("delete the file error:" + e.getMessage());
        }
    }
}
