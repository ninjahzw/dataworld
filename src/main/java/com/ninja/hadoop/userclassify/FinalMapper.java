package com.ninja.hadoop.userclassify;

import com.ninja.hadoop.mapreduce.VertexWritable;
import com.ninja.hadoop.util.EncoderHandler;
import com.ninja.hadoop.util.HbaseUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/17/13
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class FinalMapper extends Mapper<Text, VertexWritable, Text, Text> {

    HbaseUtil hbase;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        hbase = HbaseUtil.getInstance("uid_relation");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        // close hbase
        if (null != hbase){
            hbase.close();
        }
    }

    private static final String UID_SPLIT = "|";

    @Override
    protected void map(Text key, VertexWritable value, Context context) throws IOException, InterruptedException {
        // means this is the final step. write the data into hbase.
        String tuid = value.getSideData().toString();
        if (tuid == null || tuid.equals("null")){
            return;
        }

        String uid = value.getVertexId().toString();

        //write to hbase
        hbase.put(tuid,"uid","1", EncoderHandler.encodeByMD5(uid));
    }

}
