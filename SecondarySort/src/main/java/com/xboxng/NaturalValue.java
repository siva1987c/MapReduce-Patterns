package com.xboxng;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiang on 12/21/14.
 */
public class NaturalValue implements Writable/*, Comparable<NaturalValue>*/ {
    private long timestamp;
    private double price;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /*
    @Override
    public int compareTo(NaturalValue o) {
        if (this.timestamp < o.timestamp) {
            return -1;
        } else if (this.timestamp > o.timestamp) {
            return 1;
        } else {
            return 0;
        }
    }
    */

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.price = dataInput.readDouble();
    }
}
