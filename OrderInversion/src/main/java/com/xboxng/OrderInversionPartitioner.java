package com.xboxng;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by qiang on 1/5/15.
 */
public class OrderInversionPartitioner extends Partitioner<WordPair, IntWritable> {
    @Override
    public int getPartition(WordPair wordPair, IntWritable intWritable, int i) {
        return wordPair.getWord().hashCode() % i;
    }
}
