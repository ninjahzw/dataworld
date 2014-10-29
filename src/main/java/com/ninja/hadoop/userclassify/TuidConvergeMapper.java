package com.ninja.hadoop.userclassify;

import net.sf.json.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/21/13
 * Time: 4:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class TuidConvergeMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static int MIN_COOKIE_NUM = 1000;
    public static int MAX_COOKIE_NUM = 4000;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ///////////  !!!  ////////////
        String[] values = (value.toString() +  " ").split("\t");
        if (values.length < 2){
            return;
        }
        String tuid = values[1];
        // skip bad record.
        if (tuid.length() != 32){
            return;
        }

        ///////////  !!!  ////////////
        String cookieStr = values[values.length - 2];
        if (cookieStr.trim().equals("")){
            return;
        }
        String time = values[1];
        JSONObject cookieJson;
        try {
            cookieJson = JSONObject.fromObject(cookieStr);
        } catch (Exception e){
            return;
        }

        Iterator<String> cookies = cookieJson.keys();
        JSONObject usefulCoolies = new JSONObject();

        while (cookies.hasNext()){
            String cookieKey = cookies.next();
            int intKey;
            try {
                intKey = Integer.parseInt(cookieKey);
            } catch (Exception e){
                continue;
            }
            if ( intKey >= MIN_COOKIE_NUM && intKey < MAX_COOKIE_NUM ){
                usefulCoolies.accumulate(cookieKey,cookieJson.get(cookieKey));
            }
        }

        context.write(new Text(tuid),new Text(usefulCoolies.toString()+"\t"+time));
    }
}
