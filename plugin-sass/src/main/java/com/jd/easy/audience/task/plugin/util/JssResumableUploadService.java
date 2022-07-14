package com.jd.easy.audience.task.plugin.util;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.jd.jss.JingdongStorageService;
import com.jd.jss.StorageClient;
import com.jd.jss.client.JsonMessageConverter;
import com.jd.jss.client.Request;
import com.jd.jss.client.Request.Builder;
import com.jd.jss.domain.ObjectListing;
import com.jd.jss.domain.ObjectSummary;
import com.jd.jss.domain.StorageObject;
import com.jd.jss.domain.multipartupload.*;
import com.jd.jss.exception.StorageServerException;
import com.jd.jss.http.JssInputStreamEntity;
import com.jd.jss.http.Method;
import com.jd.jss.http.Scheme;
import com.jd.jss.http.StorageHttpResponse;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.util.*;

/**
 * @Classname JssResumableUploadService
 * @Description 反编译JSS的ResumableUploadService
 * @Date 2021/3/18 14:08
 * @Author
 */
public class JssResumableUploadService {
    private static Logger log = LoggerFactory.getLogger(JssResumableUploadService.class);
    private StorageClient client;
    private String bucket;
    private String key;
    private String uploadId;
    private String contentMd5;
    private String mergeFlag;
    private String dir;
    private JingdongStorageService jss;

    public JssResumableUploadService(JingdongStorageService jss, StorageClient client, String bucketName, String key, String mergeFlag, String dir) {
        this.client = client;
        this.bucket = bucketName;
        this.key = key;
        this.jss = jss;
        this.dir = dir;
        this.uploadId = this.getExistingUploadId(client, bucketName, key);
    }

    public String multipartUpload() throws IOException {
        String uploadId = initMultipartUploadResult();
        List<UploadPartResult> uploadPartResultList = newMultipartUpload(uploadId);
        return completeMultipartUpload(uploadPartResultList, uploadId);
    }

    public List<UploadPartResult> newMultipartUpload(String uploadId) throws IOException {
        List<UploadPartResult> uploadPartResults = Lists.newArrayList();
        Map<String, String> chunkFileMap = getChunkFileMap(jss, bucket, dir);

        Set<String> set = chunkFileMap.keySet();
        Integer min = Integer.parseInt(set.stream().findFirst().get());
        System.out.println("min :" + min);
        for (String partNum : chunkFileMap.keySet()) {
            StorageObject object = getInputStreamFromJss(jss, bucket, chunkFileMap.get(partNum));
            if (min.equals(Integer.valueOf(partNum))) {
                UploadPartResult uploadPartResult = uploadPart(bucket, key, Integer.valueOf(partNum), object.getContentLength(), object.getInputStream(), uploadId, client);
                uploadPartResults.add(uploadPartResult);
                log.info("first part Upload Part Result " + uploadPartResult);

            } else {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(object.getInputStream()));
                String firstLine = reader.readLine();
                System.out.println("header :" + firstLine);
                System.out.println("content length:" + object.getContentLength());
                InputStream in2 = new ReaderInputStream(reader);
                UploadPartResult uploadPartResult = uploadPart(bucket, key, Integer.valueOf(partNum), object.getContentLength() - firstLine.length() - 1, in2, uploadId, client);
                uploadPartResults.add(uploadPartResult);
                log.info("other part Upload Part Result " + uploadPartResult);
            }
        }

        return uploadPartResults;
    }

    public Map<String, String> getChunkFileMap(JingdongStorageService jss, String bucketName, String dir) {
        Map<String, String> chunkFileMap = new TreeMap<>((String obj1, String obj2) -> {
            int obj1Int = Integer.parseInt(obj1);
            int obj2Int = Integer.parseInt(obj2);
            return obj1Int - obj2Int;
        });
        ObjectListing objectListing = jss.bucket(bucketName).prefix(dir).listObject();
        for (ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            String fileName = objectSummary.getKey();
            if (fileName.contains("_SUCCESS")) {
                System.out.println("success FILE");
                continue;
            }
//            String filePartNum = fileName.substring(fileName.lastIndexOf("_") + 1);
            String filenamePure = fileName.substring(fileName.lastIndexOf("/") + 1);
            System.out.println("filenamePure:" + filenamePure);
            String index = filenamePure.split("-")[1];
            System.out.println("key:" + fileName + ",index=" + Integer.parseInt(index));
            chunkFileMap.put(String.valueOf(Integer.parseInt(index)), fileName);

        }
        return chunkFileMap;
    }

//    private List<UploadPartResult> resumeMultipartUpload(String uploadId) throws IOException {
//        ListPartsResult partsResult = listParts(uploadId);
//        ArrayList var2 = Lists.newArrayList();
//        long partSize = 0L;
//        Iterator iterator = partsResult.getParts().iterator();
//
//        while(iterator.hasNext()) {
//            Part part = (Part)iterator.next();
//            partSize += part.getSize();
//            UploadPartResult uploadPartResult = new UploadPartResult();
//            uploadPartResult.seteTag(part.geteTag());
//            uploadPartResult.setPartNumber(part.getPartNumber());
//            var2.add(uploadPartResult);
//        }
//
//        int var12 = var2.size() + 1;
//        log.info("resumable upload start at partNumber [" + var12 + "] and skip [" + partSize + "] bytes");
//        long var13 = this.entity.getContentLength();
//        Preconditions.checkArgument(partSize <= var13, "File has changed. please abort this multipart upload");
//        if (partSize < var13) {
//            InputStream var8 = this.entity.getContent();
//            ByteStreams.skipFully(var8, partSize);
//
//            while((long)var12 <= var13 / this.partSize) {
//                UploadPartResult var9 = this.uploadPart(var12, this.partSize, ByteStreams.limit(var8, this.partSize));
//                var2.add(var9);
//                log.info("Upload Part Result " + var9);
//                ++var12;
//            }
//
//            long var14 = var13 % this.partSize;
//            if (var14 != 0L) {
//                UploadPartResult var11 = this.uploadPart(var12, var14, var8);
//                var2.add(var11);
//            }
//        }
//
//        return var2;
//    }

    public StorageObject getInputStreamFromJss(JingdongStorageService jss, String bucketName, String objName) {
        try {
            StorageObject obj = jss.bucket(bucketName).object(objName).get();
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String initMultipartUploadResult() {
        Builder builder = Request.builder().scheme(Scheme.DEFAULT).endpoint(client.getConfig().getEndpoint());
        Request request = builder.bucket(bucket).key(key).subResource("uploads").method(Method.POST).build();
        InitMultipartUploadResult result = (InitMultipartUploadResult) client.excute(request, InitMultipartUploadResult.class);
        return result.getUploadId();
    }

    public String completeMultipartUpload(List<UploadPartResult> uploadPartResults, String uploadId) {
        CompleteMultipartUploadParts completeMultipartUploadParts = new CompleteMultipartUploadParts(uploadPartResults);
        String var4 = "";
        if (!Strings.isNullOrEmpty(mergeFlag) && !Boolean.parseBoolean(mergeFlag)) {
            try {
                MessageDigest digest = MessageDigest.getInstance("MD5");
                StringBuilder builder = new StringBuilder();
                Iterator iterator = uploadPartResults.iterator();

                while (iterator.hasNext()) {
                    UploadPartResult partResult = (UploadPartResult) iterator.next();
                    builder.append(partResult.geteTag());
                }

                byte[] bytes = builder.toString().getBytes("UTF8");
                digest.update(bytes, 0, bytes.length);
                var4 = Hex.encodeHexString(digest.digest());
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        String write = JsonMessageConverter.write(completeMultipartUploadParts);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(write.getBytes());
        InputStreamEntity inputStreamEntity = new InputStreamEntity(byteArrayInputStream, (long) byteArrayInputStream.available());
        Builder builder = Request.builder().scheme(Scheme.DEFAULT).endpoint(client.getConfig().getEndpoint());
        builder.header("x-jss-server-merge", mergeFlag);
        Request request = builder.bucket(bucket).key(key).entity(inputStreamEntity).method(Method.POST).parameter("uploadId", uploadId).build();
        log.info("Try Complete Multipart Upload with uploadId [" + uploadId + "]");
        CompleteMultipartUploadResult uploadResult = (CompleteMultipartUploadResult) client.excute(request, CompleteMultipartUploadResult.class);
        String eTag = uploadResult.geteTag();
        log.info("Complete Multipart Upload finished, the file md5 is " + eTag);
        if (!Strings.isNullOrEmpty(mergeFlag) && !Boolean.parseBoolean(mergeFlag)) {
            if (!var4.equalsIgnoreCase(eTag)) {
                throw new RuntimeException("BadDigest partsMd5 expect[" + var4 + "], but actual[" + eTag + "]");
            }

            eTag = eTag + "-1";
        }

        if (this.contentMd5 != null && (Strings.isNullOrEmpty(mergeFlag) || Boolean.parseBoolean(mergeFlag)) && !this.contentMd5.equalsIgnoreCase(eTag)) {
            throw new RuntimeException("BadDigest md5 expect[" + this.contentMd5 + "], but actual[" + eTag + "]");
        } else {
            return eTag;
        }
    }

    private ListPartsResult listParts(String uploadId) {
        Builder builder = Request.builder().scheme(Scheme.DEFAULT).endpoint(client.getConfig().getEndpoint());
        builder.bucket(bucket).key(this.key);
        Request request = builder.method(Method.GET).parameter("uploadId", uploadId).build();
        return (ListPartsResult) client.excute(request, ListPartsResult.class);
    }

    public UploadPartResult uploadPart(String bucketName, String key, int partNum, long length, InputStream content, String uploadId, StorageClient client) {
        log.info("Upload part number [" + partNum + "],length [" + length + "]");
        Builder builder = Request.builder().scheme(Scheme.DEFAULT).endpoint(client.getConfig().getEndpoint());
        Request request = builder.bucket(bucketName).key(key).entity(new JssInputStreamEntity(content, length)).
                parameter("uploadId", uploadId).parameter("partNumber", String.valueOf(partNum)).method(Method.PUT).build();
        String eTag = null;
        StorageHttpResponse response = null;

        try {
            response = (StorageHttpResponse) client.excute(request, StorageHttpResponse.class);
            eTag = (String) response.getHeades().get("ETag");
        } finally {
            if (response != null) {
                response.close();
            }
        }

        if (Strings.isNullOrEmpty(eTag)) {
            throw new StorageServerException(new SocketTimeoutException());
        } else {
            UploadPartResult uploadPartResult = new UploadPartResult();
            uploadPartResult.setPartNumber(partNum);
            uploadPartResult.seteTag(eTag.substring(1, 33));
            return uploadPartResult;
        }
    }

    public String getExistingUploadId(StorageClient client, String bucketName, String key) {
        Builder builder = Request.builder().scheme(Scheme.DEFAULT).endpoint(client.getConfig().getEndpoint());
        builder.subResource("uploads");
        builder.bucket(bucketName);
        builder.parameter("prefix", key);
        builder.parameter("maxKeys", "1");
        Request request = builder.method(Method.GET).build();
        MultipartUploadListing uploadListing = (MultipartUploadListing) client.excute(request, MultipartUploadListing.class);
        Iterator iterator = uploadListing.getUploads().iterator();

        Upload upload;
        do {
            if (!iterator.hasNext()) {
                return null;
            }

            upload = (Upload) iterator.next();
        } while (!upload.getKey().equals(key));

        return upload.getUploadId();
    }

    public void setContentMd5(String var1) {
        this.contentMd5 = var1;
    }

}