package com.ninja.hadoop.verify;

import com.ninja.hadoop.userclassify.CompletionMapper;
import com.ninja.hadoop.util.TimeUtil;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/16/13
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class TuidInfo {

    public static class InfoMapper extends Mapper<Text, Text, Text, Text> {

        Logger log = Logger.getLogger(CompletionMapper.class);

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString() + " ";
            String items[] = line.split("\t");
            Configuration conf = context.getConfiguration();
            String start = conf.get("start");
            String end = conf.get("end");
            String tuid = items[1];
            String time = items[2];
            String cookie = items[items.length-2];
            JSONObject jsonObject;
            if (cookie == null || cookie.equals("")){
                context.write(new Text(tuid),new Text(""));
            }

            try {
                jsonObject = JSONObject.fromObject(cookie);
            } catch (JSONException e) {
                e.printStackTrace();
                log.error("cookies are not valid witch are : " + cookie);
                return;
            }
            Iterator<String> cookieIt =  jsonObject.keys();
            while (cookieIt.hasNext()){

            }

            context.write(key,value);
        }
    }

    public static class InfoReducer extends Reducer<Text, Text, Text, Text> {

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
                    boolean haveUid = false;
                    for (String value : lValues){
                        String svalue = value.toString();
                        String[] svalues = svalue.split("\t");
                        String tuid = svalues[0];
                        String time = svalues[1];
                        if (day.equals(time)){
                            line.append(tuid).append(" ");
                            haveUid = true;
                        }
                    }
                    if (haveUid){
                        line.append("|");
                    } else {
                        line.append("NULL").append("|");
                    }
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

        /////////  map-reduce stage 2   ////////////

        job = new Job(conf);
        job.setNumReduceTasks(9);
        job.setJobName("ad merge");

        job.setMapperClass(InfoMapper.class);
        job.setReducerClass(InfoReducer.class);
        job.setJarByClass(InfoMapper.class);

        Path in  = inputPath1;
        Path out = new Path("/data/tuid_info/");

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
