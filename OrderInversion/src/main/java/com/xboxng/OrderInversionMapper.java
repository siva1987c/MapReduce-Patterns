package com.xboxng;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiang on 1/5/15.
 */
public class OrderInversionMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
    private final static int NEIGHBOR_WIN = 2;

    private void mapWindow(String[] tokens, int i, Context context) throws IOException, InterruptedException {
        int left = i - NEIGHBOR_WIN;
        left = left >= 0 ? left : 0;

        int right = i + NEIGHBOR_WIN;
        right = right < tokens.length ? right : tokens.length - 1;

        WordPair pairStar = new WordPair(tokens[i], "*");
        int countStar = 0;

        for (int j = left; j < right; j++) {
            if (tokens[i].equals(tokens[j])) {
                continue;
            }

            WordPair pair = new WordPair(tokens[i], tokens[j]);
            context.write(pair, new IntWritable(1));
            countStar++;
        }

        context.write(pairStar, new IntWritable(countStar));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().trim().split(" ");

        if (tokens.length < 2) {
            return;
        }

        for (int i = 0; i < tokens.length; i++) {
            mapWindow(tokens, i, context);
        }
    }
}
