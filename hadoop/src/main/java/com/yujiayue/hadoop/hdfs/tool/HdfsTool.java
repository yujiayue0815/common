package com.yujiayue.hadoop.hdfs.tool;

import com.yujiayue.hadoop.hdfs.HdfsClient;
import com.yujiayue.util.Strings;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
     * 获取文件输出流
     *
     * @param localFile  本地文件名称
     * @param remotePath 远程路劲
     * @return
     * @throws IOException
     */
    public static FileOutputStream getStream(String localFile, String remotePath) throws IOException {
        FileOutputStream outputStream = null;
        FSDataInputStream inputStream = null;
        if (Strings.notEmpty(remotePath)) {
            try {
                outputStream = new FileOutputStream(new File(localFile));
                Path path = new Path(remotePath);
                inputStream = client.open(path);
                IOUtils.copyBytes(inputStream, outputStream, client.getConf());
            } finally {
                IOUtils.closeStream(inputStream);
                HdfsClient.close();
            }
        } else {
            log.error("Remote file is null.");
        }
        return outputStream;
    }


    /**
     * 通过流上传文件
     *
     * @param inputStream 输入流
     * @param remotePath  hdfs路径
     */
    public static void putStream(FileInputStream inputStream, String remotePath) throws IOException {
        if (Strings.notEmpty(remotePath)) {
            FSDataOutputStream outputStream = null;
            try {
                Path path = new Path(remotePath);
                outputStream = client.create(path);
                IOUtils.copyBytes(inputStream, outputStream, client.getConf());
            } finally {
                IOUtils.closeStream(inputStream);
                IOUtils.closeStream(outputStream);
                HdfsClient.close();
            }
        } else {
            log.error("Remote file is null.");
        }
    }


    /**
     * 查看文件是否存在
     *
     * @param path 路径
     * @return boolean
     * @throws IOException
     */
    public static boolean exists(String path) throws IOException {
        return exists(path, false);
    }

    /**
     * 判断文件是否存在
     *
     * @param path   路径
     * @param create 不存在时是否创建
     * @return boolean
     * @throws IOException
     */
    public static boolean exists(String path, boolean create) throws IOException {
        boolean flag = false;
        try {
            try {
                flag = client.exists(new Path(path));
                if (!flag)
                    if (create)
                        mkdir(path);
            } finally {
                HdfsClient.close();
            }
        } finally {
            HdfsClient.close();
        }
        return flag;
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
                    Path remotePath = new Path(remoteFile);
                    if (client.exists(remotePath)) {
                        client.copyToLocalFile(delSrc, remotePath, new Path(localPath));
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
     * @throws IOException 如果失败抛出异常
     */
    public static void get(String remoteFile, String localPath) throws IOException {
        get(false, true, remoteFile, localPath);
    }


    /**
     * 剪切
     *
     * @param remoteFile 指要下载的文件路径
     * @param localPath  指将文件下载到的路径
     * @throws IOException 如果失败抛出异常
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
                Path pathSrc = new Path(path);
                if (client.exists(pathSrc)) {
                    client.delete(pathSrc, true);
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
                Path pathSrc = new Path(path);
                if (client.exists(pathSrc)) {
                    client.deleteOnExit(pathSrc);
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
                Path pathSrc = new Path(path);
                if (client.exists(pathSrc)) {
                    client.cancelDeleteOnExit(pathSrc);
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


    /**
     * 重命名
     *
     * @param path hdfs路径
     */
    public static void rename(String path, String repath) throws IOException {
        if (Strings.notEmpty(path)) {
            try {
                Path pathSrc = new Path(path);
                if (client.exists(pathSrc)) {
                    client.rename(pathSrc, new Path(repath));
                    log.info("Rename file " + path + ". ");
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
     * 查看目录信息,使用该方法需要手动关闭客户端
     * such as :
     * try{
     * listFiles(path)
     * }finally{
     * HdfsClient.close();
     * }
     *
     * @param path      路径
     * @param recursive 是否递归
     * @return RemoteIterator<LocatedFileStatus>
     * @throws IOException 失败抛出异常
     */
    public static RemoteIterator<LocatedFileStatus> listFiles(String path, boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> files = null;
        if (Strings.notEmpty(path)) {
            Path pathSrc = new Path(path);
            if (client.exists(pathSrc)) {
                files = client.listFiles(pathSrc, recursive);
                log.info("Rename file " + path + ". ");
            } else {
                log.info("Remote file " + path + " not exists.");
            }
        } else {
            log.error("Path file is null.");
        }
        return files;
    }

    /**
     * 查看目录状态信息
     *
     * @param path 路径
     * @return FileStatus[]
     * @throws IOException 失败抛出异常
     */
    public static FileStatus[] listStatus(String path) throws IOException {
        FileStatus[] files = null;
        if (Strings.notEmpty(path)) {
            Path pathSrc = new Path(path);
            try {
                if (client.exists(pathSrc)) {
                    files = client.listStatus(pathSrc);
                    log.info("Rename file " + path + ". ");
                } else {
                    log.info("Remote file " + path + " not exists.");
                }
            } finally {
                HdfsClient.close();
            }
        } else {
            log.error("Path file is null.");
        }
        return files;
    }


}
