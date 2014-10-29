package com.ninja.hadoop.util;

import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 11/7/13
 * Time: 3:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class TimeUtil {

    public static String timeIntervalDays(long startTime, long endTime) {
        long between = (endTime - startTime);
        long day = between / (24 * 60 * 60 * 1000);
//        long hour = (between / (60 * 60 * 1000) - day * 24);
//        long min = ((between / (60 * 1000)) - day * 24 * 60 - hour * 60);
//        long s = (between / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
//        long ms = (between - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000 - min * 60 * 1000 - s * 1000);
        return Long.toString(day);
    }

    public static long dateStringToLong(String date) throws ParseException{
        SimpleDateFormat sdf= new SimpleDateFormat("yyyyMMdd");
        Date dt2 = sdf.parse(date);
        long lTime = dt2.getTime();
        return lTime;
    }

    public static String dateLongToString(long date){
        SimpleDateFormat sdf= new SimpleDateFormat("yyyyMMdd");
        java.util.Date dt = new Date(date);
        String sDateTime = sdf.format(dt);
        return sDateTime;
    }
    /**
     * The day BEFORE {day}
     * @param date
     * @param day
     * @return
     */
    public static String getDateBefore(String date,int day) throws ParseException{

        Calendar now = Calendar.getInstance();
        SimpleDateFormat sdf= new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse(date);
        now.setTime(d);
        now.set(Calendar.DATE,now.get(Calendar.DATE)-day);

        return dateLongToString(now.getTimeInMillis());
    }

    /**
     * The day AFTER {day}
     * @param date
     * @param day
     * @return
     */
    public static String getDateAfter(String date,int day) throws ParseException{
        Calendar now = Calendar.getInstance();
        SimpleDateFormat sdf= new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse(date);
        now.setTime(d);
        now.set(Calendar.DATE,now.get(Calendar.DATE)+day);

        return dateLongToString(now.getTimeInMillis());
    }

    public static void main(String[] dd){

//        String start = "20130101";
//        StringBuffer line = new StringBuffer("");
//        String[] aaa = {"UHHDENDK\t1356969623453","UHHDENDK\t1356969623453","UHHDENDK\t1357056000000","UHHDENDK\t1357142400000"};
//        try {
//            for (int i = 0; i < 4; i++){
//                String day = TimeUtil.getDateAfter(start,i);
//                System.out.println(day);
//
//                boolean haveUid = false;
//                for (String svalue : aaa){
//                    String[] svalues = svalue.split("\t");
//                    String tuid = svalues[0];
//                    String time = TimeUtil.dateLongToString(Long.parseLong(svalues[1]));
//                    if (day.equals(time)){
//                        line.append(tuid).append(" ");
//                        haveUid = true;
//                    }
//                }
//                if (haveUid){
//                    line.append("|");
//                } else {
//                    line.append("NULL").append("|");
//                }
//            }
//        } catch (Exception e){
//
//        }
//
//        System.out.println(line.toString());

//        String start = "20130101";
//        String end = "20130130";

        try {

            System.out.println(TimeUtil.dateLongToString(1382449860563L));
            System.out.println(TimeUtil.dateLongToString(1381861898210L));
            System.out.println(TimeUtil.dateLongToString(1382458328586L));
////            System.out.println(TimeUtil.dateLongToString(Long.parseLong("1382090654179")));
////            System.out.println(TimeUtil.dateStringToLong(start));
////            System.out.println(TimeUtil.dateStringToLong(end));
////            System.out.println(TimeUtil.timeIntervalDays(TimeUtil.dateStringToLong(start), TimeUtil.dateStringToLong(end)));
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
