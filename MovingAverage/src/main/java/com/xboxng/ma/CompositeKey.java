package com.xboxng.ma;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiang on 1/9/15.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    private String symbol;
    private String date;

    public CompositeKey() {

    }

    @Override
    public int compareTo(CompositeKey that) {
        int v = this.symbol.compareTo(that.symbol);
        return v == 0 ? this.date.compareTo(that.date) : v;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(symbol);
        dataOutput.writeUTF(date);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.symbol = dataInput.readUTF();
        this.date = dataInput.readUTF();
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
