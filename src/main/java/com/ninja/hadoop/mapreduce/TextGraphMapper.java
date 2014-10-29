package com.ninja.hadoop.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TextGraphMapper extends Mapper<Text, Text, Text, VertexWritable> {

    Logger log = Logger.getLogger(TextGraphMapper.class);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        VertexWritable vertex = new VertexWritable();
        Text realKey = null;
        if (value == null || value.equals("")) {
            return;
        }

        String cookiesStr = value.toString();

        String[] cookies = cookiesStr.split(",");

        if (cookiesStr.trim().equals("")){
            return;
        }

        int currentCount = 0;
        for (String cookie : cookies) {
            if (cookie.trim().equals("")){
                continue;
            }
            if (currentCount == 0) {
                realKey = new Text(cookie);
                // set the vertex
                vertex.checkAndSetMinimalVertex(realKey);
                // add to edges
                vertex.addVertex(realKey);
            } else {
                Text temp = new Text(cookie);
                vertex.checkAndSetMinimalVertex(temp);
                // add to edges
                vertex.addVertex(temp);
            }
            currentCount++;
        }

        // data that wanted to be grouped by same kind of cookie.
        if (key != null && !key.equals("null")){
            vertex.setSideData( new Text(key.toString()) );
        }

        if (realKey != null && vertex != null){
            context.write(realKey, vertex);
        }

        for (Text edge : vertex.getEdges()) {
            if (null != edge){
                context.write(edge, vertex.makeMessage());
            }
        }
    }

}
