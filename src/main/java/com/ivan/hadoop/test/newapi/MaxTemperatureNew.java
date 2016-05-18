package com.ivan.hadoop.test.newapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: ivan
 * Date: 16/5/18
 * Time: 下午2:39
 * Version: 1.0
 */
public class MaxTemperatureNew {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(MaxTemperatureNew.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setMapperClass(MaxTemperatureMapperNew.class);
        job.setReducerClass(MaxTemperatureReducerNew.class);

        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
