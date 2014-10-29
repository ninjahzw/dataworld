package com.ninja.hadoop;

import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 3:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHive2 {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.1.153:10000/", "hive", "123456");
        System.out.println("-----------------shit-------------------");
        Statement stmt = con.createStatement();
        String tableName = "user";
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
