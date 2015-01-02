package com.xboxng.phase2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by qiang on 1/1/15.
 */
public class Phase2Reducer extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<String>();
        for (Text value : values) {
            set.add(value.toString());
        }

        context.write(key, new IntWritable(set.size()));
    }
}
