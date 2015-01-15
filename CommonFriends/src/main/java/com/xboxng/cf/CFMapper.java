package com.xboxng.cf;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by qiang on 1/14/15.
 */
public class CFMapper extends Mapper<LongWritable, Text, CFKey, TextArrayWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length != 2) {
            return;
        }
        String p1 = tokens[0].trim();

        String[] friends = tokens[1].trim().split(" +");
        TextArrayWritable outputValue = new TextArrayWritable(friends);

        for (String friend : friends) {
            CFKey cfKey = new CFKey();
            cfKey.setPair(p1, friend);
            context.write(cfKey, outputValue);
        }
    }
}
