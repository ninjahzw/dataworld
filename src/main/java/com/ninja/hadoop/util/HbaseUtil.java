package com.ninja.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/22/13
 * Time: 5:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseUtil {

    Logger logger = Logger.getLogger(HbaseUtil.class);

    private static HbaseUtil hbaseUtil;

    HTable table;
    String tableName;
    Configuration config;

    private HbaseUtil( String tableName){
        config = HBaseConfiguration.create();

        this.tableName = tableName;
        try {
            table = new HTable(config, this.tableName);
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (Exception e) {
            logger.error(" check hbase failed " + e.getMessage());
        }
    }

    public static HbaseUtil getInstance(String tableName){
        if (null == hbaseUtil) {
            hbaseUtil = new HbaseUtil(tableName);
        } else {
            if (hbaseUtil.tableName.equals(tableName)){
                return hbaseUtil;
            } else {
                return new HbaseUtil(tableName);
            }
        }
        return hbaseUtil;
    }

    public void put(String key, String cf, String colName , String colValue){

        try {
            Put p = new Put(key.getBytes());
            p.add(cf.getBytes(), colName.getBytes(),colValue.getBytes());

            table.put(p);
        } catch (IOException e) {
            logger.error("check table failed" + e.getMessage());
        } finally {
            //this.close();
        }
    }

    public void flush(){
        if (null != table){
            try {
                table.flushCommits();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    public void close(){
        try {
            if (null != table){
                table.flushCommits();
                table.close();
            }
        } catch (IOException e) {
            logger.error("table close failed" + e.getMessage());
        }
    }

}
