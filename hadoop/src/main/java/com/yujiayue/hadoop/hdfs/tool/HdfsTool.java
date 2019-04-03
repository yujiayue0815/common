package com.yujiayue.hadoop.hdfs.tool;

import com.yujiayue.hadoop.hdfs.HdfsClient;
import com.yujiayue.util.Strings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * hdfs 客户端工具
 */
public class HdfsTool {

    private static final Logger log = LoggerFactory.getLogger(HdfsTool.class);
    private static FileSystem client = HdfsClient.get();

    /**
     * 上传文件
     *
     * @param srcFile    本地文件
     * @param remotePath hdfs路径
     */
    public static void put(String srcFile, String remotePath) throws IOException {
        if (Strings.notEmpty(srcFile)) {
            if (Strings.notEmpty(remotePath)) {
                try {
                    client.copyFromLocalFile(new Path(srcFile), new Path(remotePath));
                } finally {
                    HdfsClient.close();
                }
            } else {
                log.error("Remote file is null.");
            }
        } else {
            log.error("Local file is null.");
        }
    }

    /**
     * 创建文件夹
     *
     * @param remotePath hdfs路径
     * @return boolean
     */
    public static boolean mkdir(String remotePath) throws IOException {
        boolean mkdirs = false;
        if (Strings.notEmpty(remotePath)) {
            try {
                log.info("Create dir " + remotePath + " from to HDFS.");
                mkdirs = client.mkdirs(new Path(remotePath));
            } finally {
                HdfsClient.close();
            }
        } else {
            log.error("Remote file is null.");
        }
        return mkdirs;
    }


    /**
     * 下载操作
     *
     * @param delSrc      指是否将原文件删除
     * @param useRawLocal 是否开启文件校验
     * @param remoteFile  指要下载的文件路径
     * @param localPath   指将文件下载到的路径
     */
    public static void get(boolean delSrc, boolean useRawLocal, String remoteFile, String localPath) throws IOException {

        if (Strings.notEmpty(remoteFile)) {
            if (Strings.notEmpty(localPath)) {
                try {
                    if (client.exists(new Path(remoteFile))) {
                        client.copyToLocalFile(delSrc, new Path(remoteFile), new Path(localPath));
                        log.info("Download file " + remoteFile + " to local " + localPath);
                    } else {
                        log.info("Remote file " + remoteFile + " not found.");
                    }
                } finally {
                    HdfsClient.close();
                }
            } else {
                log.error("Local file is null.");
            }
        } else {
            log.error("Remote file is null.");
        }
    }

    /**
     * 下载
     *
     * @param remoteFile 指要下载的文件路径
     * @param localPath  指将文件下载到的路径
     * @throws IOException
     */
    public static void get(String remoteFile, String localPath) throws IOException {
        get(false, true, remoteFile, localPath);
    }


    /**
     * 剪切
     *
     * @param remoteFile 指要下载的文件路径
     * @param localPath  指将文件下载到的路径
     * @throws IOException
     */
    public static void cut(String remoteFile, String localPath) throws IOException {
        get(true, true, remoteFile, localPath);
    }

    /**
     * 删除
     *
     * @param path hdfs路径
     */
    public static void delete(String path) throws IOException {

        if (Strings.notEmpty(path)) {
            try {
                if (client.exists(new Path(path))) {
                    client.delete(new Path(path), true);
                    log.info("Delete file " + path + ". ");
                } else {
                    log.info("Remote file " + path + " not exists.");
                }
            } finally {
                HdfsClient.close();
            }
        } else {
            log.error("Path file is null.");
        }
    }

    /**
     * 标记删除
     *
     * @param path hdfs路径
     */
    public static void markDelete(String path) throws IOException {

        if (Strings.notEmpty(path)) {
            try {
                if (client.exists(new Path(path))) {
                    client.deleteOnExit(new Path(path));
                    log.info("Mark delete file " + path + ". ");
                } else {
                    log.info("Remote file " + path + " not exists.");
                }
            } finally {
                HdfsClient.close();
            }
        } else {
            log.error("Path file is null.");
        }
    }


    /**
     * 取消标记删除
     *
     * @param path hdfs路径
     */
    public static void cancelMarkDelete(String path) throws IOException {
        if (Strings.notEmpty(path)) {
            try {
                if (client.exists(new Path(path))) {
                    client.cancelDeleteOnExit(new Path(path));
                    log.info("Cancel mark's delete file " + path + ". ");
                } else {
                    log.info("Remote file " + path + " not exists.");
                }
            } finally {
                HdfsClient.close();
            }
        } else {
            log.error("Path file is null.");
        }
    }

}
