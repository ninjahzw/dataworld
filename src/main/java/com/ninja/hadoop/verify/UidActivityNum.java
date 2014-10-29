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
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class UidActivityNum {

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
            Configuration conf = context.getConfiguration();
            String start = conf.get("start");
            String end = conf.get("end");
            int interval;
            StringBuffer line = new StringBuffer("");
            // because we have to iterate the list for many times, so we must set the items to a list so that they will not disappear after a single iteration.
            Set<String> lValues = new HashSet<String>();
            for (Text value : values){
                String svalue = value.toString();
                String[] svalues = svalue.split("\t");
                String tuid = svalues[0];
                String time = TimeUtil.dateLongToString(Long.parseLong(svalues[1]));
                String one = tuid + "\t" + time;
                lValues.add(one);
            }

            try {
                interval = Integer.parseInt(TimeUtil.timeIntervalDays(TimeUtil.dateStringToLong(start), TimeUtil.dateStringToLong(end)));
                for (int i = 0; i < interval; i++){
                    String day = TimeUtil.getDateAfter(start,i);
                    int tuids = 0;
                    for (String value : lValues){
                        String svalue = value.toString();
                        String[] svalues = svalue.split("\t");
                        String time = svalues[1];

                        if (day.equals(time)){
                            tuids ++;
                        }
                    }
                    line.append(tuids).append("|");

                }
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
            context.write(key,new Text(line.toString()));
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
        job.setNumReduceTasks(9);

        //this must be here, otherwise those Mapper or Reducer classes will not be found.
        job.setJarByClass(UidActivityNum.class);     // class that contains mapper

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

        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);
        job.setJarByClass(MergeMapper.class);

        Path in  = outputPath;
        Path out = new Path(args[4]);

        FileInputFormat.addInputPath(job, in);
        if (fs.exists(out)){
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }
}
