package com.ivan.hadoop.test.oldapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ivan
 * Date: 16/5/18
 * Time: 下午2:19
 * Version: 1.0
 */
public class MaxTemperature {

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(MaxTemperature.class);
        conf.setJobName("Max temperature");

        System.out.println(args[1]);
        System.out.println(args[2]);
        FileInputFormat.addInputPath(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        conf.setMapperClass(MaxTemperatureMapper.class);
        conf.setReducerClass(MaxTemperatureReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}