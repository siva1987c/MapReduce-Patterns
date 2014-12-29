package com.xboxng.aggregate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qiang on 12/28/14.
 */
public class AggregateKeyDriver extends Configured implements Tool {
    @Override
    public int run(String... args) throws Exception {
        Job job = new Job(getConf());
        job.setJobName("AggregateKey");

        job.setJarByClass(AggregateKeyDriver.class);
        job.setJarByClass(AggregateKeyMap.class);
        job.setJarByClass(AggregateKeyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(AggregateKeyMap.class);
        job.setReducerClass(AggregateKeyReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        System.exit(ToolRunner.run(new AggregateKeyDriver(), args));
    }
}
