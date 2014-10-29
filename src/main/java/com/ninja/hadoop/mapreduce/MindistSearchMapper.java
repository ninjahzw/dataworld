package com.ninja.hadoop.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MindistSearchMapper extends Mapper<Text, VertexWritable, Text, VertexWritable> {

    @Override
    protected void map(Text key, VertexWritable value, Context context) throws IOException, InterruptedException {

        context.write(key, value);

        if (value.isActivated()) {
            VertexWritable writable = new VertexWritable();
            for (Text neighborVertex : value.getEdges()) {
                if (!neighborVertex.toString().equals(value.getVertexId().toString())) {
                    writable.setVertexId(value.getVertexId());
                    writable.setEdges(null);
                    if (neighborVertex == null || neighborVertex.toString().trim().equals("")){
                        continue;
                    }
                    context.write(neighborVertex, writable);
                }
            }
        }
    }

}
