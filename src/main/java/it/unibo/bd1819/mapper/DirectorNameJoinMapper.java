package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map all the DirectorTuple adding a prefix to distinguish them on the reduce phase
 */
public class DirectorNameJoinMapper  extends Mapper<Text, Text, Text, Text> {

    public final static String DIRECTOR_JOIN_PREFIX = "drjnprx";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(DIRECTOR_JOIN_PREFIX + value.toString()));
    }
}
