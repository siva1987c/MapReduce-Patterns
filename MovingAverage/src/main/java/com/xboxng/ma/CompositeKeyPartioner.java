package com.xboxng.ma;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by qiang on 1/9/15.
 */
public class CompositeKeyPartioner extends Partitioner<CompositeKey, DoubleWritable> {
    @Override
    public int getPartition(CompositeKey compositeKey, DoubleWritable doubleWritable, int n) {
        return compositeKey.getSymbol().hashCode() % n;
    }
}
