package test;

import com.jd.easy.audience.common.oss.OssClientTypeEnum;
import com.jd.easy.audience.common.oss.OssConfig;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.util.JssResumableUploadService;
import com.jd.jss.Credential;
import com.jd.jss.JingdongStorageService;
import com.jd.jss.StorageClient;
import com.jd.jss.client.ClientConfig;
import com.jd.jss.client.StorageHttpClient;
import com.jd.jss.domain.ObjectListing;
import com.jd.jss.domain.ObjectSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author cdxiongmei
 * @version 1.0
 * @description oneid关联创建标签数据集验证
 * @date 2022/5/31 1:52 PM
 */
public class WriteTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteTest.class);
    private static final Pattern DB_TABLE_SPLIT = Pattern.compile("^[a-zA-Z0-9|_]{1,10}\\.([_|\\w]*){1}(\\.[_|\\w]*)");

    public static void main(String[] args) throws IOException {
        String accessKey = "8cFPJ3Eu8iuR9naW";
        String secretKey = "8cJkTYMSSupXAJo0mlhdCbaJ1JXpg5XIzBpbeuE3";
        String endpoint = "test.storage.jd.local";
        /**
         * 8cFPJ3Eu8iuR9naW
         * 8cJkTYMSSupXAJo0mlhdCbaJ1JXpg5XIzBpbeuE3
         * ae-test
         */
        String ossKey = "xmtest202206/dataset/label/11703";
//        String bucket = "pre-cdp";
        String bucket = "ae-test";
        System.out.println("endPoint:" + endpoint + ", accessKey:" + accessKey + ", outputpath:" + secretKey);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSessionWithOssTest("TestReadStep", endpoint, accessKey, secretKey, OssClientTypeEnum.JFS);
//        Dataset<Row> dataOld = sparkSession.read().option("header", "true").format("csv").load("file:///Users/cdxiongmei/Downloads/xm0623.csv");

        Dataset<Row> dataOld = sparkSession.read().option("header", "true").format("csv").load("jfs://" + bucket + "/" + ossKey + "/temp");
        dataOld.cache();

        /**
         * 分片上传
         */
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        OssConfig config = new OssConfig(endpoint, accessKey, secretKey);
        JdCloudOssUtil.initOssConf(configuration, config, OssClientTypeEnum.JFS);
        long partNums = 10;

        //合并文件
        JingdongStorageService jss = createjss(endpoint, accessKey, secretKey);
        JdCloudOssUtil.deleteOssDir(jss, bucket, ossKey, "");
        System.out.println("分片上传，partnum：" + partNums + ",ossKey:" + ossKey);
        dataOld.repartition((int) partNums).write().option("header", "true").format("csv").save("jfs://" + bucket + "/" + ossKey + "/temp");
        StorageClient client = getClient(endpoint, accessKey, secretKey);
        JssResumableUploadService uploadService = new JssResumableUploadService(jss, client,
                bucket, ossKey + "/" + "xm0623.csv", "false", ossKey + "/temp");

        uploadService.multipartUpload();
//        删除分片文件
        ObjectListing objectListing = jss.bucket(bucket).prefix(ossKey + "/temp").listObject();
        for (ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println("prepare delete key=" + objectSummary.getKey());
            jss.bucket(bucket).object(objectSummary.getKey()).delete();
        }
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
}
