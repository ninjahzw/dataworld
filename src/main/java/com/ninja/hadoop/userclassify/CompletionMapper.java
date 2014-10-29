package com.ninja.hadoop.userclassify;

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
public class CompletionMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        // the tuid string that will be appended to the tail of the cookies to be forwarded.
        final String tuidString = ",tuid:" + key.toString();

        String cookiesStr = value.toString();

        // the future cookie key.
        String firstCookie = "";
        int depth = 0;
        String[] cookies = cookiesStr.split(",");
        if (cookies.length > 0){
            String firstItem = cookies[0];
            // write the whole item
            context.write(new Text(firstItem), new Text(cookiesStr + tuidString));
        } else {
            return;
        }

        for (String cookie : cookies){
            depth ++;
            // first cookie
            if (1 == depth){
                firstCookie = cookie;
            } else {
                // write each edge with the key
                context.write(new Text(cookie),new Text(firstCookie));
            }
        }
    }

}