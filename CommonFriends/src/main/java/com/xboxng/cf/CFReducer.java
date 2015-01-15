package com.xboxng.cf;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by qiang on 1/14/15.
 */
public class CFReducer extends Reducer<CFKey, TextArrayWritable, Text, Text> {
    @Override
    public void reduce(CFKey key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        Set<Writable> set = new HashSet<>();

        for (TextArrayWritable value : values) {
            if (set.size() == 0) {
                set.addAll(Arrays.asList(value.get()));
            } else {
                set.retainAll(Arrays.asList(value.get()));
            }
        }

        String v1 = "(" + key.getP1() + ", " + key.getP2() + ") -> ";
        context.write(new Text(v1), new Text(set.toString()));
    }
}
