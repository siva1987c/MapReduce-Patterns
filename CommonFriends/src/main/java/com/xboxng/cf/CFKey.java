package com.xboxng.cf;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiang on 1/11/15.
 */
public class CFKey implements WritableComparable<CFKey>{
    public String p1;
    public String p2;

    @Override
    public int compareTo(CFKey o) {
        int v1 = this.p1.compareTo(o.p1);
        int v2 = this.p2.compareTo(o.p2);
        if (v1 == 0 && v2 ==0) {
            return 0;
        } else {
            return v1 == 0 ? v2 : v1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(p1);
        dataOutput.writeUTF(p2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.p1 = dataInput.readUTF();
        this.p2 = dataInput.readUTF();
    }

    public void setPair(String p1, String p2) {
        if (p1 == null || p2 == null) {
            throw new IllegalArgumentException("neither p1 nor p2 can be null");
        }

        if (p1.compareTo(p2) <= 0) {
            this.p1 = p1;
            this.p2 = p2;
        } else {
            this.p1 = p2;
            this.p2 = p1;
        }
    }

    public String getP1() {
        return p1;
    }

    public String getP2() {
        return p2;
    }

    @Override
    public int hashCode() {
        return this.p1.hashCode() * 7 + this.p2.hashCode() + 13;
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof CFKey)) {
            return false;
        }

        CFKey that = (CFKey)other;
        return  this.p1.equals(that.p1) && this.p2.equals(that.p2);
    }
}
