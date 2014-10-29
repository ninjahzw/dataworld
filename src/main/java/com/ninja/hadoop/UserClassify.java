package com.ninja.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserClassify {
    public static class MapperClass extends
            Mapper<Text, Text, Text, IntWritable> {

        private IntWritable score = new IntWritable();

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            score.set(Integer.parseInt(value.toString()));
            context.write(key, score);
        }
    }

    public static class ReducerClass extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable score = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            int sum = 0;
            int i = 0;
            for (IntWritable val : values) {
                sum += val.get();
                i++;
            }
            int averageScore = sum / i;
            score.set(averageScore);
            context.write(key, score);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf);
        conf.set("mapred.job.tracker", "192.18.1.140:9001");
        conf.set("fs.default.name", "hdfs://192.168.1.140:9000");
        conf.set("hadoop.job.ugi", "hadoop");
        conf.set("Hadoop.tmp.dir", "/user/gqshao/temp/");


        job.setJarByClass(UserClassify.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, "/user/hive/warehouse/main/dt=20131008/");
        FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/resultt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
