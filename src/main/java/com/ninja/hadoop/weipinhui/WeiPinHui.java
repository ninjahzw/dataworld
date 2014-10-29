package com.ninja.hadoop.weipinhui;

import com.ninja.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class WeiPinHui {

    public static final String[] HOSTS = {"taobao.com","tmall.com","jd.com","yixun.com","dangdang.com","jumei.com","vancl.com","suning.com"};

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] values = value.toString().split("\t");
            if (values.length <= 4){
                return;
            }

            String time = values[2];
            String tuid = values[1];
            String url = values[3];
            for (String host : HOSTS){
                if (url.contains(host)){
                    context.write(new Text(tuid), new Text(host+"\t"+time));
                }
            }

        }
    }

    public static class HbaseMapper extends TableMapper<Text, Text> {

        public static final byte[] CF = "uid".getBytes();
        public static final byte[] ATTR1 = "1".getBytes();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String key = Bytes.toString(row.get());
            byte[] bytes = value.getValue(CF, ATTR1);
            if (null == bytes || bytes.length == 0){
                System.out.println("------- bytes is null --------");
                return;
            }
            String val = new String(bytes);
            context.write(new Text(key), new Text(val));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            String uid = null;
            String hostTime = null;
            for (Text value : values) {
                String sValue = value.toString();
                if (sValue.indexOf("com")!= -1) { // tuid
                    hostTime = sValue;
                } else { // ad account
                    uid = sValue;
                }
            }

            if (uid != null && hostTime != null) {
                String[] items = hostTime.split("\t");
                String host = items[0];
                String time = items[1];
                String date;
                try {
                     date = TimeUtil.dateLongToString(Long.parseLong(time));
                } catch (Exception e){
                    e.printStackTrace();
                    return;
                }

                context.write(new Text(uid + "_" + date),new Text(host + "\t" + time));
            }
        }
    }

    public static class CalMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class CalReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            String[] uidDate = key.toString().toString().split("_");
            String date = uidDate[1];
            Map<String,String> orderMap = new TreeMap<String,String>();


            for (Text value : values){
                String[] hostTime = value.toString().split("\t");
                String host = hostTime[0];
                String time = hostTime[1];
                orderMap.put(time,host);
            }

            int before = 0;
            int middle = 0;
            int after  = 0;
            for (String one : HOSTS){
                Set<String> inner = new TreeSet<String>();
                Set<String> keys = orderMap.keySet();
                for (String oneKey : keys){
                    if (oneKey.equals(one)){
                        inner.add(orderMap.get(oneKey));
                    }
                }
                String low = "0";
                String high = "0";
                if (inner.size() != 0){
                    int index = 0;
                    while (inner.iterator().hasNext()){
                        if (index == 0){
                            low = inner.iterator().next();
                        } else {
                            high = inner.iterator().next();
                        }
                    }
                }
                long lLow = Long.parseLong(low);
                long lHigh = Long.parseLong(high);

                for (String oneKey : keys){
                    long time = Long.parseLong(oneKey);
                    if (time < lLow){
                        before ++;
                    }
                    if (lLow < time && time < lHigh){
                        middle ++;
                    }
                    if (time > lHigh){
                        after ++;
                    }
                }
                context.write(new Text(one + "_" + date), new Text(before + "\t" + middle + "\t" + after));
            }

        }
    }



    public static class ResultMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class ResultReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            String[] uidDate = key.toString().toString().split("_");
            String date = uidDate[1];
            Map<String,String> orderMap = new TreeMap<String,String>();


            for (Text value : values){
                String[] hostTime = value.toString().split("\t");
                String host = hostTime[0];
                String time = hostTime[1];
                orderMap.put(time,host);
            }

            int before = 0;
            int middle = 0;
            int after  = 0;
            for (String one : HOSTS){
                Set<String> inner = new TreeSet<String>();
                Set<String> keys = orderMap.keySet();
                for (String oneKey : keys){
                    if (oneKey.equals(one)){
                        inner.add(orderMap.get(oneKey));
                    }
                }
                String low = "0";
                String high = "0";
                if (inner.size() != 0){
                    int index = 0;
                    while (inner.iterator().hasNext()){
                        if (index == 0){
                            low = inner.iterator().next();
                        } else {
                            high = inner.iterator().next();
                        }
                    }
                }
                long lLow = Long.parseLong(low);
                long lHigh = Long.parseLong(high);

                for (String oneKey : keys){
                    long time = Long.parseLong(oneKey);
                    if (time < lLow){
                        before ++;
                    }
                    if (lLow < time && time < lHigh){
                        middle ++;
                    }
                    if (time > lHigh){
                        after ++;
                    }
                }
                context.write(new Text(one + "_" + date), new Text(before + "\t" + middle + "\t" + after));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Path inputPath1 = new Path(args[0]);
        Path outputPath = new Path("/data/before_after/tmp");
        //Path inputPath1 = new Path("/user/hive/warehouse/main/dt=20131008/");
        Path inputPath2 = new Path("/hbase");
        //Path outputPath = new Path("/user/hive/warehouse/output/9");

        String tableName = "uid_relation";

        Configuration config = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        // to pass the parameters to the M-R tasks.

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        @SuppressWarnings("deprecation")
        Job job = new Job(config);
        job.setNumReduceTasks(9);

        //this must be here, otherwise those Mapper or Reducer classes will not be found.
        job.setJarByClass(WeiPinHui.class);     // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes("uid"));

        TableMapReduceUtil.initTableMapperJob(
                tableName,        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                HbaseMapper.class,   // mapper
                Text.class,       // mapper output key
                Text.class,       // mapper output value
                job);

        TableMapReduceUtil.addDependencyJars(job);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);    // reducer class
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //FileInputFormat.addInputPath(job, inputPath1);
        // inputPath1 here has no effect for HBase table
        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, JoinMapper.class);
        MultipleInputs.addInputPath(job, inputPath2,  TableInputFormat.class, HbaseMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);

        /////////  map-reduce stage 2   ////////////

        job = new Job(conf);
        job.setNumReduceTasks(9);
        job.setJobName("ad merge");

        job.setMapperClass(CalMapper.class);
        job.setReducerClass(CalReducer.class);
        job.setJarByClass(CalMapper.class);

        Path in  = outputPath;
        Path out = new Path("/data/before_after/tmp1");

        FileInputFormat.addInputPath(job, in);
        if (fs.exists(out)){
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        /////////  map-reduce stage 3   ////////////

        job = new Job(conf);
        job.setNumReduceTasks(9);
        job.setJobName("ad merge");

        job.setMapperClass(ResultMapper.class);
        job.setReducerClass(ResultReducer.class);
        job.setJarByClass(ResultMapper.class);

        in  = out;
        out = new Path("/data/before_after/result");

        FileInputFormat.addInputPath(job, in);

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }
}
