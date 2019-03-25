package com.yujiayue.redis.constant;

public enum ConnectionEnum {

    /**
     * 单链接
     */
    JEDIS(1),

    /**
     * 链接池
     */
    POOL(2),
    /**
     * 主从
     */
    SENTINE(3),
    /**
     * 集群
     */
    CLUSTER(4),
    ;

    ConnectionEnum(int type) {
        this.type = type;
    }

    private int type;

    public int getType() {
        return type;
    }
}
