package com.xboxng.ma;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qiang on 1/9/15.
 */
public class MADriver extends Configured implements Tool {
    @Override
    public int run(String... args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(MADriver.class);
        job.setJarByClass(MAMapper.class);
        job.setJarByClass(MADriver.class);

        job.setMapperClass(MAMapper.class);
        job.setReducerClass(MAReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(CompositeKeyPartioner.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        ToolRunner.run(new MADriver(), args);
    }
}
