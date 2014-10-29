package com.ninja.hadoop.userclassify;

import com.ninja.hadoop.util.HbaseUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/9/13
 * Time: 7:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    public static void main(String[] args) throws Exception {

        String url = "00018DFE2696E88046934D4265A11EC5\t1381054615453\thttp://www.sadri.com.cn/\twww.sadri.com.cn";

        System.out.println("1382090560977".length());

    }


    public static String timeInterval(long startTime, long endTime) {
        long between = (startTime - endTime);
        long day = between / (24 * 60 * 60 * 1000);
        long hour = (between / (60 * 60 * 1000) - day * 24);
        long min = ((between / (60 * 1000)) - day * 24 * 60 - hour * 60);
        long s = (between / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
        long ms = (between - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000 - min * 60 * 1000 - s * 1000);
        return day + " days";
    }
}
