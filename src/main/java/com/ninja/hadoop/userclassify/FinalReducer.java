package com.ninja.hadoop.userclassify;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/17/13
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class FinalReducer extends Reducer<Text, Text, Text, Text> {

    private static final String UID_SPLIT = "|";

    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

        Logger logger = Logger.getLogger(FinalReducer.class);

        for (Text value : values){
            String sValue = value.toString();
            System.out.println("Finally -------------------- " + key.toString() + " | " +  sValue);
        }
    }

}
