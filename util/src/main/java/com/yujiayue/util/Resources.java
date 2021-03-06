package com.yujiayue.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:46
 * @description : 资源获取
 */
public interface Resources {


    /**
     * 通过bundle 获取配置
     *
     * @param path
     * @return
     */
    static ResourceBundle getBundle(String path) {
        if (Strings.notEmpty(path)) {
            return ResourceBundle.getBundle(path);
        } else
            return null;
    }


    /**
     * 通过bandle 获取配置文件信息
     *
     * @param path
     * @param key
     * @return
     */
    static String getString(String path, String key) {
        String s = null;
        try {
            ResourceBundle bundle = getBundle(path);
            if (bundle != null)
                s = bundle.getString(key);
        } catch (Exception e) {
        }
        return s;
    }


    /**
     * 获取资源配置文件,实时加载
     *
     * @param fileName
     * @return
     */
    static Properties get(String fileName) {
        Properties props = new Properties();
        if (fileName != null && !"".equals(fileName)) {
            InputStream in = null;
            try {
                in = Resources.class.getClassLoader().getResourceAsStream(fileName);
                props.load(in);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (null != in) {
                        in.close();
                    }
                } catch (IOException e) {
                }
            }
        }
        return props;
    }

    /**
     * 获取指定值的信息
     *
     * @param fileName
     * @param key
     * @return
     */
    static Object get(String fileName, String key) {
        Properties properties = get(fileName);
        return properties.get(key);
    }
}
