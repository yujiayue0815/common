package com.yujiayue.hadoop.hdfs;


import com.yujiayue.hadoop.constant.HdfsEnum;
import com.yujiayue.util.Resources;
import com.yujiayue.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:38
 * @description : hdfs 客户端
 */
public class HdfsClient {
    private static final Logger log = LoggerFactory.getLogger(HdfsClient.class);
    /**
     * hdfs 配置文件
     */
    private static final String HDFS_CONFIG = "hdfs";

    private static volatile ThreadLocal<FileSystem> fileSystemLocal = new ThreadLocal<>();

    static {
        if (fileSystemLocal.get() == null) {
            synchronized (HdfsClient.class) {
                if (fileSystemLocal.get() == null) {
                    try {
                        init();
                    } catch (InterruptedException | IOException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }


    /**
     * 初始化
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    private static void init() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        String user = Resources.getString(HDFS_CONFIG, HdfsEnum.USER.getValue());
        String url = Resources.getString(HDFS_CONFIG, HdfsEnum.HADOOP_URL.getValue());
        if (Strings.notEmpty(url)) {
            if (Strings.notEmpty(user)) {
                fileSystemLocal.set(FileSystem.get(new URI(url), configuration, user));
            } else {
                log.error("User is none.");
            }
        } else {
            log.error("Url is none.");
        }
    }

    /**
     * 获取 文件系统
     *
     * @return
     */
    public static FileSystem get() {
        return fileSystemLocal.get();
    }

    /**
     * 关闭资源文件
     */
    public static void close() {
        if (fileSystemLocal.get() != null) {
            try {
                fileSystemLocal.get().close();
                fileSystemLocal.remove();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
