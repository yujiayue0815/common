package com.yujiayue.redis.connect;

import com.yujiayue.redis.constant.ConfigFileEnum;
import com.yujiayue.util.Resources;
import redis.clients.jedis.Jedis;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:40
 * @description : 获取一个连接
 *
 */
public class Jediss {

    private static Jedis jedis = null;

    static {
        String host = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.HOST.getValue());
        String port = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.PORT.getValue());
        if (jedis == null) {
            synchronized (Jediss.class) {
                if (jedis == null) {
                    assert port != null;
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
     * @return Jedis
     */
    static Jedis get() {

        if (jedis != null)
            return jedis;
        else
            return null;
    }
}