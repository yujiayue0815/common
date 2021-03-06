package com.yujiayue.redis.connect;


import com.yujiayue.redis.constant.ConnectionEnum;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:40
 * @description : 获取连接
 *
 */
public interface Connections {

    /**
     * 获取链接资源,不包含集群连接
     *
     * @param connectType 连接类型
     * @return Jedis
     */
    static Jedis get(int connectType) {
        if (connectType > 0 && connectType <= 3) {
            if (ConnectionEnum.JEDIS.getType() == connectType)
                return Jediss.get();
            else if (ConnectionEnum.POOL.getType() == connectType)
                return Pool.get();
            else
                return Sentine.get();
        } else {
            System.out.println("When using redis clustering, use get methods that have no parameters.");
            return null;
        }
    }

    /**
     * 获取集群的连接
     *
     * @return JedisCluster
     */
    static JedisCluster get() {
        return Cluster.get();
    }
}
