package com.yujiayue.redis.connect;

import com.yujiayue.redis.constant.ConfigFileEnum;
import com.yujiayue.util.Resources;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Set;

/**
 * 高可用哨兵模式连接，读写分离
 */
public class Sentine {


    private static volatile JedisSentinelPool jedisSentinelPool = null;

    static {
        init();
    }

    private Sentine() {
    }

    /**
     * 初始化连接池
     */
    private static void init() {

        String host = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.HOST.getValue());
        String port = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.PORT.getValue());
        String masterName = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.MASTER_NAME.getValue());
        String split = ConfigFileEnum.SPLIT.getValue();
        assert port != null;
        String[] ports = port.split(split);
        if (ports.length > 1) {
            if (jedisSentinelPool == null) {
                synchronized (Sentine.class) {
                    assert host != null;
                    String[] hosts = host.split(split);
                    Set<String> address = new HashSet<>();
                    if (hosts.length == 1) {
                        for (String port1 : ports) {
                            address.add(host + ":" + port1);
                        }
                    } else if (hosts.length == ports.length) {
                        for (int i = 0; i < ports.length; i++) {
                            address.add(hosts[i] + ":" + ports[i]);
                        }
                    } else {
                        System.out.println("redis config error");
                        return;
                    }
                    if (jedisSentinelPool == null) {
                        jedisSentinelPool = new JedisSentinelPool(masterName, address);
                    }
                }
            }

        } else {
            System.out.println("redis config error");
        }


    }

    /**
     * 获取连接配置
     *
     * @return Jedis
     */
    static Jedis get() {

        if (jedisSentinelPool != null)
            return jedisSentinelPool.getResource();
        else
            return null;
    }
}