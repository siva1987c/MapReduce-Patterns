package com.xboxng.cf;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qiang on 1/14/15.
 */
public class CFDriver extends Configured implements Tool {
    @Override
    public int run(String... args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(CFDriver.class);
        job.setJarByClass(CFMapper.class);
        job.setJarByClass(CFReducer.class);

        job.setMapperClass(CFMapper.class);
        job.setMapOutputKeyClass(CFKey.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setReducerClass(CFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        ToolRunner.run(new CFDriver(), args);
    }
}
