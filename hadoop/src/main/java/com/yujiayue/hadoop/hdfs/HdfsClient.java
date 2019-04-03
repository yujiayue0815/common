package com.yujiayue.hadoop.hdfs;


import com.yujiayue.hadoop.constant.HdfsEnum;
import com.yujiayue.util.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hdfs 客户端
 */
public class HdfsClient {
    private static final Logger log = LoggerFactory.getLogger(HdfsClient.class);

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
     * hdfs 配置文件
     */
    private static final String HDFS_CONFIG = "hdfs";


    /**
     * 初始化
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    private static void init() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        String replication = Resources.getString(HDFS_CONFIG, HdfsEnum.REPLICATION.getValue());
        configuration.set(HdfsEnum.REPLICATION.getValue(), replication);
        String user = Resources.getString(HDFS_CONFIG, HdfsEnum.USER.getValue());
        String url = Resources.getString(HDFS_CONFIG, HdfsEnum.HADOOP_URL.getValue());
        fileSystemLocal.set(FileSystem.get(new URI(url), configuration, user));
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
