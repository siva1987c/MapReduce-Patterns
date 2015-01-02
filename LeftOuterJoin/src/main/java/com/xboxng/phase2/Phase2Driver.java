package com.xboxng.phase2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qiang on 1/1/15.
 */
public class Phase2Driver extends Configured implements Tool {
    @Override
    public int run(String...args) throws Exception {
        Job job = new Job(this.getConf());

        job.setJarByClass(Phase2Driver.class);
        job.setJarByClass(Phase2Mapper.class);
        job.setJarByClass(Phase2Reducer.class);

        job.setMapperClass(Phase2Mapper.class);
        job.setReducerClass(Phase2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        System.exit(ToolRunner.run(new Phase2Driver(), args));
    }
}
