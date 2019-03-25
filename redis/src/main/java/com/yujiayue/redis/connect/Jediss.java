package com.yujiayue.redis.connect;

import com.yujiayue.redis.constant.ConfigFileEnum;
import com.yujiayue.util.Resources;
import redis.clients.jedis.Jedis;

import java.util.Properties;

/**
 * 获取一个连接
 */
public class Jediss{

    private static Jedis jedis = null;

    static {
        String host = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(),ConfigFileEnum.HOST.getValue());
        String port = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(),ConfigFileEnum.PORT.getValue());
        synchronized (Jediss.class) {
            if (jedis == null) {
                synchronized (Jediss.class) {
                    jedis = new Jedis(host, Integer.parseInt(port));
                }
            }

        }
    }

    private Jediss() {
    }

    /**
     * 获取连接配置
     *
     * @return
     */
    public static Jedis get() {

        if (jedis != null)
            return jedis;
        else
            return null;
    }
}