package com.xboxng.ma;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiang on 1/9/15.
 */
public class MAMapper extends Mapper<LongWritable, Text, CompositeKey, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().trim().split(",");
        if (tokens.length == 3) {
            String symbol = tokens[0];
            String date  = tokens[1];
            String price = tokens[2];

            CompositeKey compositeKey = new CompositeKey();
            compositeKey.setSymbol(symbol);
            compositeKey.setDate(date);

            context.write(compositeKey, new DoubleWritable(Double.parseDouble(price)));
        }
    }
}
