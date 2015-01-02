package com.xboxng.phase1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by qiang on 1/1/15.
 */
public class CompositeKeyPartitioner extends Partitioner<CompositeKey, Text> {
    @Override
    public int getPartition(CompositeKey compositeKey, Text text, int i) {
        return compositeKey.getCustomer().hashCode() % i;
    }
}
