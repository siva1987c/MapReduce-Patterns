package com.xboxng.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * Created by qiang on 12/28/14.
 */
public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
    private int N = 10;
    private TopNFilter<Integer, String> filter = null;

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 2) {
                filter.put(Integer.valueOf(tokens[1]), tokens[0].trim());
            }
        }

        Map<Integer, String> topN = filter.topN();
        for (Integer freq : filter.topN().keySet()) {
            context.write(new IntWritable(freq), new Text(topN.get(freq)));
        }
    }

    @Override
    protected void setup(Context context) {
        this.filter = new TopNFilter<>(N);
    }
}
