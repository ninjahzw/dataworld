package com.ninja.hadoop.userclassify;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/22/13
 * Time: 11:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class CompletionReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        Set<String> setValues = new HashSet<String>();

        for (Text value : values){
            String[] items = value.toString().split(",");
            // find the real item
            if (items.length >= 1){
                // find the real item
                String[] tuidkv = items[items.length - 1].split(":");
                if (tuidkv[0].equals("tuid")){
                    Text tuid = new Text(tuidkv[1]);
                    int index = 0;
                    StringBuffer sb = new StringBuffer("");
                    for (String item : items){
                        if (index == items.length -1){
                            break;
                        }
                        if (index == items.length - 2){
                            sb.append(item);
                        } else {
                            sb.append(item).append(",");
                        }
                        index ++;
                    }
                    String cookieStr = sb.toString();
                    context.write(tuid,new Text(cookieStr));
                    continue;
                }
                for (String item : items){
                    setValues.add(item);
                }
            }
        }

        StringBuffer sb = new StringBuffer();
        sb.append(key.toString());
        int index = 0;
        for (String value : setValues){
            if (index == 0){
                sb.append(",");
            }
            if (index == setValues.size() - 1) {
                sb.append(value);
            } else {
                sb.append(value).append(",");
            }
            index ++;
        }
        String cookies = sb.toString();

        context.write(new Text("null"),new Text(cookies));
    }

}
