package com.xboxng;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by qiang on 1/5/15.
 */
public class OrderInversionReducer extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {
    int totalCount = 0;

    @Override
    public void reduce(WordPair key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        if (key.getNeighbor().equals("*")) { //
            totalCount = 0;
            for (IntWritable c : counts) {
                totalCount += c.get();
            }
        } else {
            int count = 0;
            for (IntWritable c: counts) {
                count += c.get();
            }

            context.write(key, new DoubleWritable((double)count / totalCount));
        }
    }
}
