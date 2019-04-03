package com.yujiayue.hadoop.constant;

/**
 * hdfs 配置
 */
public enum HdfsEnum {
    /**
     * 用户
     */
    USER("user"),

    /**
     * url
     */
    HADOOP_URL("url");

    HdfsEnum(String value) {
        this.value = value;
    }

    private String value;

    public String getValue() {
        return value;
    }}
