package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Text, Text, IntWritable, Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
       context.write(new IntWritable(Integer.parseInt(value.toString())), key);
    }

}
