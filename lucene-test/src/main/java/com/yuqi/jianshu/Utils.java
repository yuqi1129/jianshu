package com.yuqi.jianshu;

/**
 * Author yuqi
 * Time 9/8/19
 **/
public class Utils {
    public static String escapeString(final String s) {
        if (null == s) {
            return null;
        }

        return s.replace("`", "");
    }
}
