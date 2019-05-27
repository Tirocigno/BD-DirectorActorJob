package it.unibo.bd1819.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        for(Text value: values) {
            context.write(value, key);
        }
    }
}
