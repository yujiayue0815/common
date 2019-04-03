package com.yujiayue.util;

import java.text.DecimalFormat;

/**
 * @author : 余嘉悦
 * @date : 2019/4/3 15:46
 * @description : 数字工具类
 *
 */
public interface Numbers {


    /**
     * 数字格式转换
     *
     * @param data
     * @param length
     * @param prefix
     * @return
     */
    static String format(int data, int length, String prefix) {
        if (length <= 0) {
            throw new IllegalArgumentException("length is error");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(prefix);
        }

        DecimalFormat decimalFormat = new DecimalFormat(sb.toString());
        return decimalFormat.format(data);
    }


    /**
     * 格式化数字当位数不够时补0
     *
     * @param data
     * @param length
     * @return
     */
    static String format(int data, int length) {
        return format(data, length, "0");
    }
}
