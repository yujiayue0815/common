package com.yujiayue.redis.connect;

import com.yujiayue.redis.constant.ConfigFileEnum;
import com.yujiayue.util.Resources;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 集群连接方式
 */
public class Cluster {


    private static volatile JedisCluster jedisCluster = null;

    static {
        init();
    }

    private Cluster() {
    }

    /**
     * 初始化连接池
     */
    private static synchronized void init() {

        String host = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.HOST.getValue());
        String port = Resources.getString(ConfigFileEnum.CONFILE_FILE_NAME.getValue(), ConfigFileEnum.PORT.getValue());
        String split = ConfigFileEnum.SPLIT.getValue();
        assert port != null;
        String[] ports = port.split(split);
        if (ports.length > 1) {
            if (jedisCluster == null) {
                assert host != null;
                String[] hosts = host.split(split);
                Set<HostAndPort> clusterSet = new HashSet<>();
                if (hosts.length == 1) {
                    for (String port1 : ports) {
                        clusterSet.add(new HostAndPort(host, Integer.parseInt(port1)));
                    }
                } else if (hosts.length == ports.length) {
                    for (int i = 0; i < ports.length; i++) {
                        clusterSet.add(new HostAndPort(hosts[i], Integer.parseInt(ports[i])));
                    }
                } else {
                    System.out.println("redis config error");
                    return;
                }
                jedisCluster = new JedisCluster(clusterSet);
            }
        } else {
            System.out.println("redis config error");
        }

    }

    /**
     * 获取连接资源
     *
     * @return JedisCluster
     */
    static JedisCluster get() {
        if (jedisCluster != null)
            return jedisCluster;
        else
            return null;
    }

}