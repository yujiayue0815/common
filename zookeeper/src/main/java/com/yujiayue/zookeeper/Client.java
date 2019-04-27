package com.yujiayue.zookeeper;

import com.yujiayue.util.Resources;
import com.yujiayue.util.Strings;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.yujiayue.zookeeper.constant.ZookeeperConfig.*;


/**
 * @author : 余嘉悦
 * @date : 2019/4/12 17:59
 * @description : zookeeper 客户端
 */
public class Client {
    private static final Logger log = LoggerFactory.getLogger(Client.class);
    private static ThreadLocal<ZooKeeper> zooKeeperThreadLocal = new ThreadLocal<ZooKeeper>() {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        };

        @Override
        protected ZooKeeper initialValue() {
            ZooKeeper zooKeeper = null;
            try {
                if (CANBEREADONLY != null)
                    if (SESSION_ID != null) {
                        zooKeeper = new ZooKeeper(URL, TIME_OUT, watcher, SESSION_ID, PASSWORD.getBytes(), CANBEREADONLY);
                    } else {
                        zooKeeper = new ZooKeeper(URL, TIME_OUT, watcher, CANBEREADONLY);
                    }
                else {
                    if (SESSION_ID != null) {
                        zooKeeper = new ZooKeeper(URL, TIME_OUT, watcher, SESSION_ID, PASSWORD.getBytes());
                    } else {
                        zooKeeper = new ZooKeeper(URL, TIME_OUT, watcher);
                    }
                }
                zooKeeperThreadLocal.set(zooKeeper);
                log.info("Connect zookeeper success ,url is :" + URL);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return zooKeeper;
        }
    };


    /**
     * zookeeper 连接地址
     */
    private static String URL = "127.0.0.1:2181";

    /**
     * 连接超时时间 单位：毫秒数
     */
    private static Integer TIME_OUT = 2000;

    /**
     * 是否只读的客户端
     */
    private static Boolean CANBEREADONLY = null;

    /**
     * zookeeper 会话编号
     */
    private static Long SESSION_ID = null;

    /**
     * zookeeper 密码
     */
    private static String PASSWORD = null;

    /**
     * Init zookeeper user's setting config.
     */
    static {
        String url = Resources.getString(CONFIG, CONFIG_URL);
        if (Strings.notEmpty(url))
            URL = url;
        String timeOut = Resources.getString(CONFIG, CONFIG_TIME_OUT);
        if (Strings.notEmpty(timeOut))
            TIME_OUT = Integer.valueOf(timeOut);

        String canBeReadOnly = Resources.getString(CONFIG, CONFIG_CAN_BE_READ_ONLY);
        if (Strings.notEmpty(canBeReadOnly))
            CANBEREADONLY = Boolean.valueOf(canBeReadOnly);

        String sessionId = Resources.getString(CONFIG, CONFIG_SESSION_ID);
        if (Strings.notEmpty(sessionId))
            SESSION_ID = Long.valueOf(sessionId);

        String password = Resources.getString(CONFIG, CONFIG_PASSWORD);
        if (Strings.notEmpty(password))
            PASSWORD = password;
    }

    private Client() {
    }

    /**
     * Create zNode
     *
     * @param path       zookeeper's zNode path
     * @param data       zookeeper's zNode value
     * @param acl        this zNode  permission
     * @param createMode this zNode create to mode
     *                   as:PERSISTENT,PERSISTENT_SEQUENTIAL,
     *                   EPHEMERAL,EPHEMERAL_SEQUENTIAL
     * @return this zNode information
     */
    static String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        String s = null;
        ZooKeeper zooKeeper = get();
        try {
            s = zooKeeper.create(path, data, acl, createMode);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return s;
    }


    /**
     * The asynchronous version of create.
     *
     * @param path       zookeeper's zNode path
     * @param data       zookeeper's zNode value
     * @param acl        this zNode  permission
     * @param createMode this zNode create to mode
     *                   as:PERSISTENT,PERSISTENT_SEQUENTIAL,
     *                   EPHEMERAL,EPHEMERAL_SEQUENTIAL
     * @param cb         call back
     * @param ctx
     */
    static void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        String s = null;
        ZooKeeper zooKeeper = get();
        try {
            zooKeeper.create(path, data, acl, createMode, cb, ctx);
        } finally {
            close();
        }
    }

    /**
     * @param path
     * @param watcher
     * @return
     */
    static List<String> getChildren(String path, Watcher watcher) {
        ZooKeeper zooKeeper = get();
        List<String> s = null;
        try {
            s = zooKeeper.getChildren(path, watcher);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return s;
    }


    /**
     * Children list name.
     *
     * @param path Zookeeper node path.
     * @return
     */
    static List<String> getChildren(String path) {
        ZooKeeper zooKeeper = get();
        List<String> s = null;
        try {
            s = zooKeeper.getChildren(path, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return s;
    }

    /**
     * 获取zookeeper客户端
     *
     * @return ZooKeeper
     * @throws IOException
     */
    static ZooKeeper get() {
        return zooKeeperThreadLocal.get();
    }

    /**
     * Close zookeeper.
     */
    static void close() {
        ZooKeeper zooKeeper = zooKeeperThreadLocal.get();
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                zooKeeperThreadLocal.remove();
            }
            log.info("Zookeeper client is close.");
        }
    }

}
