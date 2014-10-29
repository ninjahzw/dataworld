package com.ninja.hadoop;

import com.ninja.hadoop.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/13/13
 * Time: 5:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHbase {
    public static void main(String[] args) throws IOException {

        HbaseUtil hbaseUtil = HbaseUtil.getInstance("uid_relation");

        hbaseUtil.put("1","uid","1","1");
        
        hbaseUtil.close();

    }
}
