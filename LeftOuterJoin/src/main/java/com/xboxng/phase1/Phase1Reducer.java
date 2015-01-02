package com.xboxng.phase1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by qiang on 1/1/15.
 */
public class Phase1Reducer extends Reducer<CompositeKey, Text, Text, Text> {
    @Override
    public void reduce(CompositeKey user, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text addr = null;
        for (Text value : values) {
            if (addr == null) {
                addr = new Text(value.toString());
            } else {
                context.write(value, addr);
            }
        }
    }
}
