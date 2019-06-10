package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Simple mapper to allow the shuffle and the sorting of the data via Partitioner.
 */
public class SortMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

    public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
       context.write(key, value);
    }
}
