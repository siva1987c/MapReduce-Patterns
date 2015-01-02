package com.xboxng.phase1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qiang on 1/1/15.
 */
public class Phase1Driver extends Configured implements Tool {
    @Override
    public int run(String... args) throws Exception {
        Job job = new Job(getConf());
        job.setJobName("LeftOuterJoin Phase1");

        job.setPartitionerClass(CompositeKeyPartitioner.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);

        job.setJarByClass(Phase1Driver.class);
        job.setJarByClass(TransactionMapper.class);
        job.setJarByClass(UserMapper.class);
        job.setJarByClass(Phase1Reducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setReducerClass(Phase1Reducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        System.exit(ToolRunner.run(new Phase1Driver(), args));
    }
}
