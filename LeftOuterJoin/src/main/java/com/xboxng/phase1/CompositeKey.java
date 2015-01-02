package com.xboxng.phase1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiang on 1/1/15.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    private String customer;
    private String type;

    public CompositeKey() {

    }

    public CompositeKey(String customer, String type) {
        this.customer = customer;
        this.type = type;
    }

    @Override
    public int compareTo(CompositeKey o) {
        int comp = this.customer.compareTo(o.customer);
        if (comp == 0) {
            comp = this.type.equalsIgnoreCase("A") ? -1 : 1;
        }
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(customer);
        dataOutput.writeUTF(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.customer = dataInput.readUTF();
        this.type = dataInput.readUTF();
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
