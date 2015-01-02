package com.xboxng.phase2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiang on 1/1/15.
 */
public class Phase2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        if (tokens.length == 2) {
            context.write(new Text(tokens[0]), new Text(tokens[1]));
        }
    }
}
