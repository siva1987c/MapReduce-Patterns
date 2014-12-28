package com.xboxng;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;

/**
 * Created by qiang on 12/22/14.
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString().trim(), ",");
        if (tokens.length == 3) {
            LocalDate localDate = LocalDate.parse(tokens[1].trim());
            long timestamp = localDate.toEpochDay();
            CompositeKey reducerKey = new CompositeKey();
            reducerKey.setStockSymbol(tokens[0].trim());
            reducerKey.setTimestamp(timestamp);

            NaturalValue reducerValue = new NaturalValue();
            reducerValue.setTimestamp(timestamp);
            reducerValue.setPrice(Double.parseDouble(tokens[2].trim()));

            context.write(reducerKey, reducerValue);
        }
    }
}
