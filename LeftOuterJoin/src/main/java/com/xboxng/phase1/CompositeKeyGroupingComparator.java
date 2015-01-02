package com.xboxng.phase1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by qiang on 1/1/15.
 */
public class CompositeKeyGroupingComparator extends WritableComparator {
    protected CompositeKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey key1 = (CompositeKey)a;
        CompositeKey key2 = (CompositeKey)b;

        return key1.getCustomer().compareTo(key2.getCustomer());
    }
}
