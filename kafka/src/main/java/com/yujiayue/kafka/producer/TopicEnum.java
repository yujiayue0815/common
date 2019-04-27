package com.yujiayue.kafka.producer;

public enum TopicEnum {
    FIRST("first")
    ;

    TopicEnum(String value) {
        this.value = value;
    }

    private String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
