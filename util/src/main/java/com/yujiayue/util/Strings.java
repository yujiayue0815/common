package com.yujiayue.util;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:47
 * @description : 字符串处理
 */
public interface Strings {

    /**
     * 不等于空
     *
     * @param s
     * @return
     */
    static boolean notEmpty(String s) {
        if (s != null) {
            if (s.length() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 不等于空
     *
     * @param s
     * @return
     */
    static boolean empty(String s) {
        return !notEmpty(s);
    }
}
