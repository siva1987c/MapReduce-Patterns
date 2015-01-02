package com.xboxng;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;

/**
 * Created by qiang on 12/22/14.
 */
public class SecondarySortReducer extends Reducer<CompositeKey, NaturalValue, Text, Text> {
    @Override
    public void reduce(CompositeKey key, Iterable<NaturalValue> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder  = new StringBuilder();
        for (NaturalValue value : values) {
            builder.append("(");
            LocalDate date = LocalDate.ofEpochDay(value.getTimestamp());
            builder.append(date.toString());
            double price = value.getPrice();
            builder.append(", " + price + ")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
    }
}
