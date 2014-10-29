package com.ninja.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MindistSearchJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        int depth = 1;
        Configuration conf = new Configuration();
        conf.set("recursion.depth", depth + "");
        Job job = new Job(conf);
        job.setJobName("Mindist Search");

        job.setMapperClass(TextGraphMapper.class);
        job.setReducerClass(MindistSearchReducer.class);
        job.setJarByClass(TextGraphMapper.class);

        Path in = new Path("/user/hive/warehouse/main/dt=20131008/test_copy_2");
        Path out = new Path(args[1]+"/depth_1");

        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out))
            fs.delete(out, true);

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VertexWritable.class);

        job.waitForCompletion(true);

        long counter = job.getCounters()
                .findCounter(MindistSearchReducer.UpdateCounter.UPDATED).getValue();
        depth++;
        while (counter > 0) {
            conf = new Configuration();
            conf.set("recursion.depth", depth + "");
            job = new Job(conf);
            job.setJobName("Mindist Search " + depth);

            job.setMapperClass(MindistSearchMapper.class);
            job.setReducerClass(MindistSearchReducer.class);
            job.setJarByClass(MindistSearchMapper.class);

            in = new Path( args[1] + "/depth_" + (depth - 1) + "/");
            out = new Path( args[1] + "/depth_" + depth + "/");

            FileInputFormat.addInputPath(job, in);
            if (fs.exists(out))
                fs.delete(out, true);

            FileOutputFormat.setOutputPath(job, out);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(VertexWritable.class);

            job.waitForCompletion(true);
            depth++;
            counter = job.getCounters()
                    .findCounter(MindistSearchReducer.UpdateCounter.UPDATED).getValue();
        }

    }

}
