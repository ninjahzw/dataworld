package com.ninja.hadoop.userclassify;


import com.ninja.hadoop.mapreduce.MindistSearchMapper;
import com.ninja.hadoop.mapreduce.MindistSearchReducer;
import com.ninja.hadoop.mapreduce.TextGraphMapper;
import com.ninja.hadoop.mapreduce.VertexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserClassify {

    private static int SAME_VALUE = 0;

//    static final Logger log = Logger.getLogger(UserClassify.class);

//    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//        @Override
//        public void map(LongWritable key, Text value, Context context)
//                throws IOException, InterruptedException {
//
//            String[] values = value.toString().split("\\|");
//            String tuid = values[0];
//            String cookies = values[values.length - 1];
//
//            context.write(new Text(tuid), new Text(cookies));
//        }
//    }

//    public static class HbaseMapper extends TableMapper<Text, Text> {
//
//        public static final byte[] CF = "uid".getBytes();
//        public static final byte[] ATTR1 = "1".getBytes();
//
//        @Override
//        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
//            String key = Bytes.toString(row.get());
//            byte[] bytes = value.getValue(CF, ATTR1);`
//            if (null == bytes || bytes.length == 0){
//                System.out.println("------- bytes is null --------");
//                return;
//            }
//            String val = new String(bytes);
//            context.write(new Text(key), new Text(val));
//        }
//    }

//    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
//
//        @Override
//        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
//            String uid = null;
//            List<String> resultList = new ArrayList<String>();
//
//            for(Text value : values){
//                // skip bad record.
//                if(null == value || value.toString().equals("")){
//                    continue;
//                }
//
//                JSONObject jsonObject = null;
//
//                try {
//                    jsonObject = new JSONObject(value.toString());
//                } catch (JSONException e) {
//                    log.error("cookies are not valid witch are : " + value.toString());
//                }
//            }
//
//        }
//    }

    public static void main(String[] args) throws Exception {

        //////////////  map-reduce stage 1   /////////////////
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJobName("Tuid Converge");
        job.setMapperClass(TuidConvergeMapper.class);
        job.setReducerClass(TuidConvergeReducer.class);
        job.setJarByClass(TuidConvergeMapper.class);
        job.setNumReduceTasks(20);
        String[] urls = args[0].toString().split(",");
        Path in;
        for (String url : urls){
            in = new Path(url);
            FileInputFormat.addInputPath(job, in);
        }

        Path out = new Path(args[1]+"/tmp1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        FileOutputFormat.setOutputPath(job, out);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        HBaseConfiguration.addHbaseResources(conf);
        try {
            User user = User.getCurrent();
            user.obtainAuthTokenForJob(conf,job);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.waitForCompletion(true);

        //////////////  map-reduce stage 2   /////////////////
        conf = new Configuration();

        job = new Job(conf);
        job.setJobName("Data Completion");
        job.setMapperClass(CompletionMapper.class);
        job.setReducerClass(CompletionReducer.class);
        job.setJarByClass(CompletionMapper.class);
        job.setNumReduceTasks(20);
        in = out;
        out = new Path(args[1]+"/tmp2");
        fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        //////////////  map-reduce stage 3   /////////////////

        int depth = 1;
        conf = new Configuration();
        conf.set("recursion.depth", depth + "");
        job = new Job(conf);
        job.setJobName("Mindist Search");

        job.setMapperClass(TextGraphMapper.class);
        job.setReducerClass(MindistSearchReducer.class);
        job.setJarByClass(TextGraphMapper.class);
        job.setNumReduceTasks(20);

        in = out;
        out = new Path(args[1]+"/depth_1");

        FileInputFormat.addInputPath(job, in);
        fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VertexWritable.class);

        job.waitForCompletion(true);

        long counter;
        depth++;
        boolean ifContinue = true;
        while (ifContinue) {
            conf = new Configuration();
            conf.set("recursion.depth", depth + "");
            job = new Job(conf);
            job.setJobName("Mindist Search " + depth);

            job.setMapperClass(MindistSearchMapper.class);
            job.setReducerClass(MindistSearchReducer.class);
            job.setJarByClass(TextGraphMapper.class);
            job.setNumReduceTasks(20);

            in = new Path( args[1] + "/depth_" + (depth - 1) + "/");
            out = new Path( args[1] + "/depth_" + depth + "/");

            FileInputFormat.addInputPath(job, in);

            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            FileOutputFormat.setOutputPath(job, out);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(VertexWritable.class);

            job.waitForCompletion(true);
            counter = job.getCounters().findCounter(MindistSearchReducer.UpdateCounter.UPDATED).getValue();
            System.out.println(">>>>> " + counter +  " depth : " + depth);

            // String increment = conf.get("increment");
            if (counter > 0){
                ifContinue = true;
            } else {
                ifContinue = false;
            }
            depth++;
        }

        //////////////// Map-Reduce stage 4 //////////////////
        conf = new Configuration();
        job = new Job(conf);
        job.setJobName("Final Step");
        job.setMapperClass(FinalMapper.class);
        job.setJarByClass(FinalMapper.class);
        job.setNumReduceTasks(20);

        in = out;
        out = new Path(args[2]);

        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        FileInputFormat.addInputPath(job, in);
        // we do not need this output.
        FileOutputFormat.setOutputPath(job,out);

        //FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        HBaseConfiguration.addHbaseResources(conf);
        try {
            User user = User.getCurrent();
            user.obtainAuthTokenForJob(conf,job);
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.waitForCompletion(true);
    }
}
