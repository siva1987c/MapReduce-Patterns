package com.xboxng.aggregate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiang on 12/28/14.
 */
public class AggregateKeyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String stringValue = value.toString().trim();
        String tokens[] = stringValue.split(",");
        if (tokens.length != 2) {
            return;
        }

        String url = tokens[0].trim();
        int freq = Integer.valueOf(tokens[1]);
        context.write(new Text(url), new IntWritable(freq));
    }
}
