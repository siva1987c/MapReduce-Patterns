package com.xboxng.ma;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by qiang on 1/9/15.
 */
public class MAReducer extends Reducer<CompositeKey, DoubleWritable, Text, DoubleWritable> {
    private static final int windowSize = 2;

    @Override
    public void reduce(CompositeKey key, Iterable<DoubleWritable> prices, Context context) throws IOException, InterruptedException {
        double [] window = new double [windowSize + 1];
        int size = 0, index = 0;
        double sum = 0;

        for (DoubleWritable price : prices) {
            // add value to window
            window[index % window.length] = price.get();
            sum += price.get();
            index++;

            if (size < windowSize) {
                size++;
                context.write(new Text(key.getSymbol() + " " + key.getDate()), new DoubleWritable(sum / size));
            } else {
                // calculate the average
                sum = sum - window[index % window.length];
                context.write(new Text(key.getSymbol() + " " + key.getDate()), new DoubleWritable(sum / size));
            }
        }

        if (size > 0 && size < windowSize) { // if prices cannot fill the window
            context.write(new Text(key.getSymbol() + " " + key.getDate()), new DoubleWritable(sum / size));
        }
    }
}
