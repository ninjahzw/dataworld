package com.ninja.hadoop.mapreduce;

import com.ninja.hadoop.util.HbaseUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MindistSearchReducer extends Reducer<Text, VertexWritable, Text, VertexWritable> {

    static final Log LOG = LogFactory.getLog(MindistSearchReducer.class);

    HbaseUtil hbase;

    public static enum UpdateCounter {
        UPDATED
    }

    boolean depthOne;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        // the recursion depth, to determine with reduce step it is.
        if (Integer.parseInt(context.getConfiguration().get("recursion.depth")) == 1){
            depthOne = true;
        }
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
    protected void reduce(Text key, Iterable<VertexWritable> values,
                          Context context) throws IOException, InterruptedException {

        if (key == null || key.toString().trim().equals("")){
            return;
        }

        Text currentMinimalKey = null;

        List<VertexWritable> realVertexes = null;

        if (depthOne) {
            realVertexes = new ArrayList<VertexWritable>();
            for (VertexWritable vertex : values) {
                if (!vertex.isMessage()) {
                    VertexWritable realVertex = vertex.clone();
                    realVertexes.add(realVertex);
                }
            }

            if(realVertexes == null){
                return;
            }

            for (VertexWritable realVertex : realVertexes ){
                realVertex.setActivated(true);
                realVertex.setVertexId(realVertex.getEdges().first());
                if (key.toString().compareTo(realVertex.getVertexId().toString()) < 0) {
                    realVertex.setVertexId(key);
                }
                context.write(key, realVertex);
            }
            context.getCounter(UpdateCounter.UPDATED).increment(1);
            //context.getConfiguration().set("increment","true");
            //context.getCounter(UpdateCounter.UPDATED).setValue(1);

        } else {
            if (realVertexes == null ) {
                realVertexes = new ArrayList<VertexWritable>();
            }

            for (VertexWritable vertex : values) {
                if (!vertex.isMessage()) {
                    VertexWritable realVertex = vertex.clone();
                    realVertexes.add(realVertex);

                } else {
                    // find the lowest vertex
                    if (currentMinimalKey == null) {
                        currentMinimalKey = new Text(vertex.getVertexId().toString());
                    } else {
                        if (currentMinimalKey.toString().compareTo(vertex.getVertexId().toString()) > 0) {
                            currentMinimalKey = new Text(vertex.getVertexId().toString());
                        }
                    }
                }
            }

            if(realVertexes == null){
                System.out.println("realVertexs are Null!");
                return;
            }

//            boolean increment = false;

            for (VertexWritable realVertex : realVertexes){
                if (currentMinimalKey != null && currentMinimalKey.toString().compareTo(realVertex.getVertexId().toString()) < 0) {
                    realVertex.setVertexId(currentMinimalKey);
                    realVertex.setActivated(true);
                    context.getCounter(UpdateCounter.UPDATED).increment(1);
                }
                context.write(key, realVertex);
            }

//
//            if (increment){
//                //context.getCounter(UpdateCounter.UPDATED).setValue(1);
//                context.getCounter(UpdateCounter.UPDATED).increment(1);
//                //context.getConfiguration().set("increment","true");
//            }

            //context.getCounter(UpdateCounter.UPDATED).setValue(1);
//            if (increment && context.getCounter(UpdateCounter.UPDATED).getValue() == 0){
//                context.getCounter(UpdateCounter.UPDATED).increment(1);
//            }

        }
    }
}
