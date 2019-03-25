package com.yujiayue.redis.constant;

/**
 * 配置文件设置
 */
public enum ConfigFileEnum {

    /**
     * 配置文件名称
     */
    CONFILE_FILE_NAME("redis.properties"),
    /**
     * ip地址
     */
    HOST("host"),
    /**
     * ip地址
     */
    MASTER_NAME("masterName"),
    /**
     * 端口号
     */
    PORT("port"),
    /**
     * 分隔符
     */
    SPLIT(",");

    ConfigFileEnum(String value) {
        this.value = value;
    }

    private String value;

    public String getValue() {
        return value;
    }
}
