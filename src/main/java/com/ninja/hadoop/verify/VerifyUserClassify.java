package com.ninja.hadoop.verify;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.ninja.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class VerifyUserClassify {

    public static class HbaseMapper extends TableMapper<Text, Text> {

        public static final byte[] CF = "uid".getBytes();
        public static final byte[] ATTR1 = "1".getBytes();

        public static final byte[] CF1 = "info".getBytes();
        public static final byte[] CF1_ATTR1 = "time".getBytes();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String key = Bytes.toString(row.get());

            byte[] bytesUid = value.getValue(CF, ATTR1);
            byte[] bytesTime = value.getValue(CF1,CF1_ATTR1);
            if (null == bytesUid || bytesUid.length == 0 || null == bytesTime || bytesTime.length == 0){
                return;
            }
            String uid = new String(bytesUid);
            String time = new String(bytesTime);
            context.write(new Text(uid), new Text(key + "\t" + time));
        }
    }

    public static class HbaseReducer extends Reducer<Text, Text, Text, Text> {

        int g1 = 0;
        int g3 = 0;
        int g7 = 0;
        int g15 = 0;
        int g20 = 0;

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);    //To change body of overridden methods use File | Settings | File Templates.
            System.out.println(" > 1 : " + g1);
            System.out.println(" > 3 : " + g3);
            System.out.println(" > 7 : " + g7);
            System.out.println(" > 15 : " + g15);
            System.out.println(" > 20 : " + g20);
        }

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
            String interval  = TimeUtil.timeIntervalDays(first,last);
            int intInterval = Integer.parseInt(interval);

            if (intInterval > 1){
                g1 ++;
            }
            if (intInterval > 3){
                g3 ++;
            }
            if (intInterval > 7){
                g7 ++;
            }
            if (intInterval > 15){
                g15 ++;
            }
            if (intInterval > 20){
                g20 ++;
            }
            //context.write(new Text("--> " + key.toString()), new Text(interval + "\t" + index + "\t" + sbResult.toString()));
            //context.write(key, new Text(interval + "\t" + index ));
        }
    }

    public static void main(String[] args) throws Exception {

        Path outputPath = new Path("/user/hive/warehouse/testresult1");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        //Path inputPath1 = new Path("/user/hive/warehouse/main/dt=20131008/");
        Path inputPath2 = new Path("/hbase");
        //Path outputPath = new Path("/user/hive/warehouse/output/9");

        String tableName = "uid_relation";

        Configuration config = HBaseConfiguration.create();

        @SuppressWarnings("deprecation")
        Job job = new Job(config);

        job.setJarByClass(VerifyUserClassify.class);     // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(5000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes("uid"));
        scan.addFamily(Bytes.toBytes("info"));

        TableMapReduceUtil.initTableMapperJob(
                tableName,        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                HbaseMapper.class,   // mapper
                Text.class,       // mapper output key
                Text.class,       // mapper output value
                job);

        TableMapReduceUtil.addDependencyJars(job);

        job.setReducerClass(HbaseReducer.class);    // reducer class
        job.setOutputFormatClass(TextOutputFormat.class);

        //FileInputFormat.addInputPath(job, inputPath1);
        // inputPath1 here has no effect for HBase table
        FileInputFormat.addInputPath(job,inputPath2);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}