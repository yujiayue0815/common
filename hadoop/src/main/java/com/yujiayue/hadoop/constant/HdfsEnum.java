package com.yujiayue.hadoop.constant;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:39
 * @description : hdfs 配置
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
