package com.jd.easy.audience.task.plugin.util.es.bean;

import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;

/**
 * EsCluster
 *
 * @author cdyaoxiangyuan
 * @date
 */
public class EsCluster {
    /**
     * es链接信息，域名:端口号,域名:端口号
     */
    String esNodes;
    /**
     * 如果esnodes不带端口号，需要单独配置端口号
     */
    String esPort;
    /**
     * 用户名
     */
    String username;
    /**
     * 密码
     */
    String password;
    /**
     * es索引
     */
    String esIndex;

    public String getEsUrl() {
        String esNodeConfig = "";
        if (esNodes.startsWith(StringConstant.HTTPHEADER) || esNodes.startsWith(StringConstant.HTTPSHEADER)) {
            esNodes = esNodes.replaceAll(StringConstant.HTTPHEADER + "|" + StringConstant.HTTPSHEADER, "");
        }
        if (esNodes.contains(StringConstant.COLON)) {
            esNodeConfig = esNodes;
        } else if (org.apache.commons.lang.StringUtils.isNotBlank(esPort)) {
            esNodeConfig = esNodes + StringConstant.COLON + esPort;
        } else {
            throw new JobInterruptedException("ES config port config is empty", "ES config port config is empty");
        }
        return esNodeConfig;
    }

    public void setEsNodes(String esNodes) {
        this.esNodes = esNodes;
    }

    public void setEsPort(String esPort) {
        this.esPort = esPort;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }
}
