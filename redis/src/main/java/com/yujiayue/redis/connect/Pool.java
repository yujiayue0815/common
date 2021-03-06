package com.yujiayue.redis.connect;

import com.yujiayue.redis.constant.ConfigFileEnum;
import com.yujiayue.util.Resources;
import com.yujiayue.util.Strings;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:40
 * @description : 链接池
 *
 */
public class Pool {
    private static volatile JedisPool jedisPool = null;

    static {
        init();
    }

    /**
     * 初始化连接池
     */
    private static void init() {

        String host = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.HOST.getValue());
        String port = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.PORT.getValue());
        if (jedisPool == null) {
            synchronized (Pool.class) {
                if (Strings.empty(host)) {
                    host = "127.0.0.1";
                }
                if (Strings.empty(port)) {
                    port = "6379";
                }
                System.out.println("connection redis host: " + host + " port:" + port);
                if (jedisPool == null) {
                    jedisPool = new JedisPool(host, Integer.parseInt(port));
                }
            }
        }
    }

    private Pool() {
    }

    /**
     * 获取连接配置
     *
     * @return Jedis
     */
     static Jedis get() {

        if (jedisPool != null)
            return jedisPool.getResource();
        else
            return null;
    }
}
