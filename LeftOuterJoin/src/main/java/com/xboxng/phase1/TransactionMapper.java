package com.xboxng.phase1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiang on 1/1/15.
 */
public class TransactionMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length == 5) {
            String customer = tokens[2].trim();
            String product = tokens[1].trim();
            context.write(new CompositeKey(customer, "P"), new Text(product));
        }
    }
}