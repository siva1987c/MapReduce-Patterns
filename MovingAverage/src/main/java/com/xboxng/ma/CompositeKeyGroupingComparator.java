package com.xboxng.ma;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by qiang on 1/9/15.
 */
public class CompositeKeyGroupingComparator extends WritableComparator {
    public CompositeKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable v1, WritableComparable v2) {
        CompositeKey k1 = (CompositeKey)v1;
        CompositeKey k2 = (CompositeKey)v2;

        return k1.getSymbol().compareTo(k2.getSymbol());
    }
}
