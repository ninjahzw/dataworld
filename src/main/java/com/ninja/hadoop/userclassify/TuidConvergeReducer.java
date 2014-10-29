package com.ninja.hadoop.userclassify;

import com.ninja.hadoop.util.HbaseUtil;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/21/13
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class TuidConvergeReducer extends Reducer<Text, Text, Text, Text> {

    Logger log = Logger.getLogger(CompletionMapper.class);

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

    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

        JSONObject jsonObject;
        Iterator<Text> iterator = values.iterator();
        Set<String> resultSet = new HashSet<String>();

        if (key == null || key.toString().equals("")){
            return;
        }

        int index = 0;
        while (iterator.hasNext()){
            Text one = iterator.next();
            String value = one.toString();
            String[] oneValues = value.split("\t");
            String cookies = oneValues[0];
            String time = oneValues[1];
//
//            if (index == 0){
//                // write item to hbase with the 'time' item.
//                // hbase.put(key.toString(),"info","time",time);
//            }
            index ++;

            if (cookies == null || cookies.equals("")){
                continue;
            }

            try {
                jsonObject = JSONObject.fromObject(cookies);
            } catch (JSONException e) {
                e.printStackTrace();
                log.error("cookies are not valid witch are : " + cookies);
                continue;
            }

            Iterator<String> cookieIt =  jsonObject.keys();
            while (cookieIt.hasNext()){
                String cookieKey = cookieIt.next();
                int intCookieKey;
                try {
                    intCookieKey = Integer.parseInt(cookieKey);
                } catch (Exception e){
                    continue;
                }
                if (intCookieKey > TuidConvergeMapper.MIN_COOKIE_NUM && intCookieKey <= TuidConvergeMapper.MAX_COOKIE_NUM){
                    resultSet.add(cookieKey + ":" + jsonObject.get(cookieKey));
                }
            }
        }
        StringBuffer resultStrBuf = new StringBuffer("");

        index = 0;
        for (String str : resultSet) {
            if (index == resultSet.size() - 1){
                resultStrBuf.append(str);
            } else {
                resultStrBuf.append(str).append(",");
            }
            index ++;
        }

        String resultStr = resultStrBuf.toString();

        context.write(key,new Text(resultStr));
    }
}
