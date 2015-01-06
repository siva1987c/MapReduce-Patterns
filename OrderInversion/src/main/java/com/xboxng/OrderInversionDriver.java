package com.xboxng;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileOutputStream;

/**
 * Created by qiang on 1/5/15.
 */
public class OrderInversionDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(OrderInversionDriver.class);
        job.setJarByClass(OrderInversionMapper.class);
        job.setJarByClass(OrderInversionReducer.class);

        job.setMapperClass(OrderInversionMapper.class);
        job.setReducerClass(OrderInversionReducer.class);

        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        ToolRunner.run(new OrderInversionDriver(), args);
    }
}
