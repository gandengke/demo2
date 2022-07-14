package com.jd.easy.audience.task.plugin.util.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jd.easy.audience.common.constant.LogicalOperatorEnum;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster;
import com.jd.easy.audience.task.plugin.util.es.sqltool.SqlCondition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Spark UDF util
 *
 * @author yuxiaoqing
 * @date 2019-09-29
 */
public class EsUtil {

    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EsUtil.class);

    /**
     * 实例化时同步锁持有对象
     */
    private static Object initLock = new Object();

    public static String deleteEsData(EsCluster esConfig, JSONObject sqlJson) {
        RestClient restClient = getRestClient(esConfig.getEsUrl(), esConfig.getUsername(), esConfig.getPassword());
        Header[] headers = getRequestHeaders(esConfig.getUsername(), esConfig.getPassword());
        // 设置请求内容
        NStringEntity reqEntity = new NStringEntity(sqlJson.toJSONString(), ContentType.APPLICATION_JSON);
        try {
            String postUrl = JdCloudOssUtil.dealPath(esConfig.getEsIndex()) + "/_delete_by_query";
            Request request = new Request("POST", File.separator + postUrl);
            request.setEntity(reqEntity);
            Response response = restClient.performRequest(request);
//            Response response = restClient.performRequest("POST", File.separator + postUrl, Collections.emptyMap(), reqEntity, headers);
            // 如果请求反馈200则继续获取反馈内容
            if (response.getStatusLine().getStatusCode() == NumberConstant.INT_200) {
                HttpEntity entity = response.getEntity();
                String respStr = EntityUtils.toString(entity);
                JSONObject jsonObj = JSONObject.parseObject(respStr);
                return jsonObj.toJavaObject(String.class);
            } else {
                LOGGER.error("postByRestClient fail:" + EntityUtils.toString(response.getEntity()));
            }
        } catch (IOException e) {
            LOGGER.error("postByRestClient error:", e);
        } finally {
            destroyClient(restClient);
        }
        return null;
    }

    public static JSONObject queryDslJSON(List<SqlCondition> conditions) {
        JSONObject dslJson = new JSONObject();
        JSONObject query = new JSONObject();
        dslJson.put("query", query);
        // 查询条件为空则匹配所有内容
        if (CollectionUtils.isEmpty(conditions)) {
            query.put("match_all", new JSONObject());
        } else {
            JSONObject bool = new JSONObject();
            query.put("bool", bool);
            Map<LogicalOperatorEnum, List<SqlCondition>> group = conditions.stream()
                    .collect(Collectors.groupingBy(SqlCondition::getLogicalOperator));
            // 包含OR，则采用should查询
            if (group.containsKey(LogicalOperatorEnum.or)) {
                List<SqlCondition> shoulds = group.get(LogicalOperatorEnum.or);
                JSONArray shouldConds = new JSONArray();
                for (SqlCondition cond : shoulds) {
                    shouldConds.add(dslCondition(cond));
                }
                bool.put("should", shouldConds);

                // 如果还包含AND，则AND条件需写入同一个子must中
                if (group.containsKey(LogicalOperatorEnum.and)) {
                    JSONObject subBool = new JSONObject();
                    JSONObject subBoolMust = new JSONObject();
                    subBool.put("bool", subBoolMust);
                    shouldConds.add(subBool);
                    List<SqlCondition> musts = group.get(LogicalOperatorEnum.and);
                    JSONArray mustConds = new JSONArray();
                    for (SqlCondition cond : musts) {
                        mustConds.add(dslCondition(cond));
                    }
                    subBoolMust.put("must", mustConds);
                }
            } else if (group.containsKey(LogicalOperatorEnum.and)) {
                List<SqlCondition> musts = group.get(LogicalOperatorEnum.and);
                JSONArray mustConds = new JSONArray();
                for (SqlCondition cond : musts) {
                    mustConds.add(dslCondition(cond));
                }
                bool.put("must", mustConds);
            }

            // key集合
            Set<LogicalOperatorEnum> keySet = group.keySet();
            Set<LogicalOperatorEnum> unsupportSet = new HashSet<>();
            for (LogicalOperatorEnum op : keySet) {
                if (op != LogicalOperatorEnum.and && op != LogicalOperatorEnum.or) {
                    unsupportSet.add(op);
                }
            }
            if (unsupportSet.size() > 0) {
                throw new RuntimeException("unsupported logical operator:" + unsupportSet);
            }
        }
        return dslJson;
    }

    /**
     * 将sql条件转换为DSL子句
     *
     * @param cond
     * @return
     */
    private static JSONObject dslCondition(SqlCondition cond) {
        JSONObject subDsl = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONObject exits = new JSONObject();
        JSONObject range = new JSONObject();
        JSONObject rangeVal = new JSONObject();
        JSONObject mustNot = new JSONObject();
        JSONObject term = new JSONObject();
        switch (cond.getCompareOpType()) {
            case EQUALITY:
                // 等于
                term.put(cond.getColumnName(), cond.getParam1());
                subDsl.put("term", term);
                break;
            case INEQUALITY:
                // 不等于
                bool.put("must_not", mustNot);
                mustNot.put("term", term);
                term.put(cond.getColumnName(), cond.getParam1());
                subDsl.put("bool", bool);
                break;
            case GT:
                range.put(cond.getColumnName(), rangeVal);
                rangeVal.put("gt", cond.getParam1());
                subDsl.put("range", subDsl);
                break;
            case GE:
                range.put(cond.getColumnName(), rangeVal);
                rangeVal.put("gte", cond.getParam1());
                subDsl.put("range", subDsl);
                break;
            case LT:
                range.put(cond.getColumnName(), rangeVal);
                rangeVal.put("lt", cond.getParam1());
                subDsl.put("range", subDsl);
                break;
            case LE:
                range.put(cond.getColumnName(), rangeVal);
                rangeVal.put("lte", cond.getParam1());
                subDsl.put("range", subDsl);
                break;
            case BETWEEN:
                range.put(cond.getColumnName(), rangeVal);
                rangeVal.put("gte", cond.getParam1());
                rangeVal.put("lte", cond.getParam2());
                subDsl.put("range", subDsl);
                break;
            case ISNULL:
                exits.put("field", cond.getColumnName());
                subDsl.put("exits", exits);
                break;
            case ISNOTNULL:
                // 不等于
                bool.put("must_not", mustNot);
                mustNot.put("exits", exits);
                exits.put("field", cond.getColumnName());
                subDsl.put("bool", bool);
                break;
            default:
                throw new RuntimeException("unsupported compare Operator type" + cond);
        }
        return subDsl;
    }

    /**
     * 获取请求header
     *
     * @param username
     * @param password
     * @return
     */
    private static Header[] getRequestHeaders(String username, String password) {
        List<Header> rtnHeader = new ArrayList<>();
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(Charset.forName("UTF-8")));
        String authHeader = "Basic " + new String(encodedAuth);
        rtnHeader.add(new BasicHeader(HttpHeaders.AUTHORIZATION, authHeader));
        rtnHeader.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
        return rtnHeader.toArray(new Header[0]);
    }

    /**
     * 生成客户端
     *
     * @param hosts
     * @return
     */
    private static RestClient getRestClient(String hosts, String user, String pass) {
        List<HttpHost> httpHosts = transToHttpHost(hosts);
        System.out.println("httphost=" + httpHosts.toString());
        RestClient restClient = RestClient.builder(httpHosts.toArray(new HttpHost[0]))
                .setFailureListener(new RestClient.FailureListener() { // 连接失败策略
                    public void onFailure(HttpHost host) {
                        LOGGER.error("init client error, host:{}", host);
                    }
                })
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() { // 认证
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        if (!StringUtils.isBlank(user)) {
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pass));
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        } else {
                            return httpClientBuilder;
                        }
                    }
                })
                .build();
        return restClient;
    }

    /**
     * 将host转换为HttpHost列表
     *
     * @param hosts 多个地址用逗号隔开，地址不用带协议，需带上端口号
     * @return
     */
    private static List<HttpHost> transToHttpHost(String hosts) {
        System.out.println("eshosts:" + hosts);
        if (StringUtils.isBlank(hosts)) {
            return null;
        }
        List<HttpHost> rtnList = new ArrayList<>();
        String[] hostArr = hosts.split(StringConstant.COMMA);
        for (String host : hostArr) {
            // 切分域名和端口号
            String[] tmp = host.split(StringConstant.COLON);
            String ip = tmp[0];
            String port = tmp.length > 1 ? tmp[1] : "80";
            HttpHost httpHost = new HttpHost(ip, Integer.parseInt(port));
            rtnList.add(httpHost);
        }
        return rtnList;
    }

    public static void destroyClient(RestClient restClient) {
        if (null != restClient) {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
