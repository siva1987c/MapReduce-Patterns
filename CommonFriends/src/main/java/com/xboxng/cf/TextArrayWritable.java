package com.xboxng.cf;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.List;

/**
 * Created by qiang on 1/14/15.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] text) {
        super(Text.class);
        Text[] textArray = new Text[text.length];
        for (int i = 0; i < text.length; i++) {
            textArray[i] = new Text(text[i]);
        }
        set(textArray);
    }
}
