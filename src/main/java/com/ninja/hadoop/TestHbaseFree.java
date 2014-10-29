package com.ninja.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/18/13
 * Time: 4:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHbaseFree {
    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        HTable table = new HTable(config, "uid_relation");
        String uid = "tuid1";
        Put p = new Put(Bytes.toBytes(uid));

        p.add("uid".getBytes(), "uid".getBytes(),"uid2".getBytes());

        table.put(p);

        table.close();

//        try{
//            Get g = new Get(Bytes.toBytes("1"));
//            Result r = table.get(g);
//            System.out.println("Row key:" + new String(r.getRow()));
//            for (KeyValue keyValue : r.raw()) {
//                System.out.println("column : " + new String(keyValue.getFamily())
//                        + "   value : " + new String(keyValue.getValue()));
//            }
//        } catch (Exception e){
//
//        } finally {
//            table.close();
//        }
    }

}
