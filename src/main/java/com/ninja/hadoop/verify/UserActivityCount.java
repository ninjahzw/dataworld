package com.ninja.hadoop.verify;

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
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserActivityCount {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] values = value.toString().split("\t");
            String time = values[2];
            String tuid = values[1];

            context.write(new Text(tuid), new Text(time));
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
            Configuration conf = context.getConfiguration();
            String start = conf.get("start");
            String end = conf.get("end");

            String uid = null;
            String time = null;
            for (Text value : values) {
                String sValue = value.toString();
                if (sValue.length() == 32) { // tuid
                    uid = sValue;
                } else if (sValue.length() == 13) { // ad account
                    time = sValue;
                }
            }

            if (uid != null && time != null) {
                long lTime = Long.parseLong(time);
                long lStart;
                long lEnd;
                try {
                    lStart = TimeUtil.dateStringToLong(start);
                    lEnd = TimeUtil.dateStringToLong(end);
                } catch (ParseException e) {
                    return;
                }
                if (lTime >= lStart && lTime <= lEnd){
                    context.write(new Text(uid), new Text(key.toString() + "\t" + time));
                }
            }
        }
    }

    public static class MergeMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class MergeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

            Set<Long> treeSet = new TreeSet<Long>();
            StringBuffer sbResult = new StringBuffer("");
            for(Text value : values){
                String[] items = value.toString().split("\t");
                try {
                    treeSet.add(Long.parseLong(items[1]));
                } catch (Exception e){
                    continue;
                }
                sbResult.append("\t");
                sbResult.append(items[0]);
            }
            Iterator<Long> it = treeSet.iterator();
            long first = 0;
            long last = 0;
            int index = 0;
            while (it.hasNext()){
                if (index == 0){
                    first = it.next();
                    last = first; // in case that there is only one item
                } else {
                    last = it.next();
                }
                index ++;
            }
            String interval  = TimeUtil.timeIntervalDays(first, last);
            int intInterval = Integer.parseInt(interval);

            if (intInterval > 1){
                context.write(new Text("g1"),new Text("1"));
            }
            if (intInterval > 3){
                context.write(new Text("g3"),new Text("1"));
            }
            if (intInterval > 7){
                context.write(new Text("g7"),new Text("1"));
            }
            if (intInterval > 15){
                context.write(new Text("g15"),new Text("1"));
            }
            if (intInterval > 20){
                context.write(new Text("g20"),new Text("1"));
            }
            if (intInterval > 20){
                context.write(new Text("g20"),new Text("1"));
            }
            context.write(new Text("total"),new Text("1"));
        }
    }

    public static class FinalMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

            int num = 0;
            for (Text t : values){
                num ++;
            }

            System.out.println(key.toString() + " : " + num);
            context.write(key,new Text(num + ""));
        }
    }

    public static void main(String[] args) throws Exception {

        Path inputPath1 = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        //Path inputPath1 = new Path("/user/hive/warehouse/main/dt=20131008/");
        Path inputPath2 = new Path("/hbase");
        //Path outputPath = new Path("/user/hive/warehouse/output/9");

        String tableName = "uid_relation";

        Configuration config = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        // to pass the parameters to the M-R tasks.
        config.set("start",args[2]);
        config.set("end",args[3]);
        conf.set("start",args[2]);
        conf.set("end",args[3]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        @SuppressWarnings("deprecation")
        Job job = new Job(config);

        //this must be here, otherwise those Mapper or Reducer classes will not be found.
        job.setJarByClass(UserActivityCount.class);     // class that contains mapper

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
        job.setNumReduceTasks(40);

        //FileInputFormat.addInputPath(job, inputPath1);
        // inputPath1 here has no effect for HBase table
        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, JoinMapper.class);
        MultipleInputs.addInputPath(job, inputPath2,  TableInputFormat.class, HbaseMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);

        /////////  map-reduce stage 2   ////////////

        job = new Job(conf);

        job.setJobName("ad merge");

        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);
        job.setJarByClass(MergeMapper.class);

        Path in  = outputPath;
        Path out = new Path(outputPath + "/1");

        FileInputFormat.addInputPath(job, in);
        if (fs.exists(out)){
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);
        job.waitForCompletion(true);


        /////////  map-reduce stage 2   ////////////

        job = new Job(conf);

        job.setJobName("last");

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        job.setJarByClass(FinalMapper.class);

        in  = out;
        out = new Path(args[4]);

        FileInputFormat.addInputPath(job, in);
        if (fs.exists(out)){
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(9);
        job.waitForCompletion(true);

    }
}
