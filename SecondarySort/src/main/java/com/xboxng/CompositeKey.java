package com.xboxng;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiang on 12/21/14.
 */
public class CompositeKey implements WritableComparable<CompositeKey>{
    private String stockSymbol;
    private long timestamp;

    @Override
    public int compareTo(CompositeKey o) {
        int v = this.stockSymbol.compareTo(o.stockSymbol);
        if (v == 0) {
            v = (int)(this.timestamp - o.timestamp);
        }
        return v;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.stockSymbol);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stockSymbol = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public String toString() {
        return stockSymbol + "\t" + timestamp;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
