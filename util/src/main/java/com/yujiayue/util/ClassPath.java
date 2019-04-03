package com.yujiayue.util;


import com.yujiayue.util.exception.TypeNotClassException;

import java.io.FileNotFoundException;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:46
 * @description : 日期格式化工具
 *
 */
public interface ClassPath {

    /**
     * Get class name and package name from to class's path.
     *
     * @param path
     * @return
     */
    static String getClassPath(String path, String packageName) throws FileNotFoundException {
        if (Strings.notEmpty(path)) {
            if (path.endsWith(".class")) {
                path = path.replace("\\", ".");
                int startIndex = 0;
                if (Strings.notEmpty(packageName)) {
                    startIndex = path.indexOf(packageName);
                } else {
                    startIndex = path.indexOf(".classes.") + ".classes.".length();
                }
                path = path.substring(startIndex);
                path = path.substring(0, path.length() - 6);
            } else {
                throw new TypeNotClassException("File type is not class!");
            }

        } else {
            throw new FileNotFoundException("Class is not exists!");
        }
        return path;
    }

    /**
     * If not know's base package name , we can this method ,but this not suggest.
     * @param path
     * @return
     * @throws FileNotFoundException
     */
    static String getClassPath(String path) throws FileNotFoundException {
        return getClassPath(path,null);
    }
}
