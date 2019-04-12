package com.yujiayue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author : 余嘉悦
 * @date : 2019/4/10 17:15
 * @description : 文件操作辅助
 */
public interface Files {

    Logger log = LoggerFactory.getLogger(Files.class);

    /**
     * create dir
     *
     * @param path
     */
    static void mkdir(String path) {
        if (Strings.notEmpty(path)) {
            File file = new File(path);
            if (!file.exists()) {
                file.mkdir();
            } else {
                log.warn(path + "dir exists");
            }
        }
    }


}
