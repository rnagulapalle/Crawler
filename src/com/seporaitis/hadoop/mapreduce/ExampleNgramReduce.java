package com.seporaitis.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExampleNgramReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> listOfValues, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException,
    InterruptedException {
        long sum = 0;
        for(LongWritable value : listOfValues) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);
    }
    
}
