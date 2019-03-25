package com.yujiayue.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期格式化工具
 */
public interface Dates {


    /**
     * 转换成日期
     *
     * @param dateStr
     * @param format
     * @return
     */
    static Date parse(String dateStr, String format) {

        SimpleDateFormat simpleFormatter = new SimpleDateFormat(format);
        Date s = null;
        try {
            s = simpleFormatter.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return s;
    }

    /**
     * 转换成固定格式
     *
     * @param date
     * @param format
     * @return
     */
    static String format(Date date, String format) {
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(format);
        return simpleFormatter.format(date);
    }

}
