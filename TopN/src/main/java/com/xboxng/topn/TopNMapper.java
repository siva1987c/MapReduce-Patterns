package com.xboxng.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiang on 12/28/14.
 */
public class TopNMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
    private int N = 10;
    private TopNFilter<Integer, String> filter = null;

    @Override
    public void map(Text key, IntWritable value, Context context) {
        int frequence = value.get();
        filter.put(frequence, key.toString() + "," + frequence);
        // unlike other mappers, don't write data to context
    }

    @Override
    protected void setup(Context context) {
        //
        this.filter = new TopNFilter<>(N);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //
        for (String str: this.filter.topN().values()) {
            context.write(NullWritable.get(), new Text(str));
        }
    }
}
