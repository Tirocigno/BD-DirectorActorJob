package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AggregateDirectorsMapper extends Mapper<Text, Text, Text, IntWritable> {
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        context.write(key, new IntWritable(Integer.parseInt(value.toString())));
    }
}
