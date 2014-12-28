package com.xboxng;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by qiang on 12/21/14.
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {

    @Override
    public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int numberOfPartitions) {
        return Math.abs(compositeKey.getStockSymbol().hashCode()) % numberOfPartitions;
    }
}
