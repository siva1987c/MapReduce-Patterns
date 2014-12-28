package com.xboxng;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by qiang on 12/21/14.
 */
public class NaturalKeyGroupingComparator extends WritableComparator{
    protected NaturalKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey ck1 = (CompositeKey)wc1;
        CompositeKey ck2 = (CompositeKey)wc2;
        return ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
    }
}
