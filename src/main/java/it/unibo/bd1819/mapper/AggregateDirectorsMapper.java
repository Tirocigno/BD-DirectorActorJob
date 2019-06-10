package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Map all the DirectorTuple using the DirectorID as a Key
 */
public class AggregateDirectorsMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] directorCountPair = value.toString().split(CUSTOM_VALUE_SEPARATOR);
        String directorID = directorCountPair[0];
        String moviesPartialCount = directorCountPair[1];
        context.write(new Text(directorID),
                new Text(key.toString() + CUSTOM_VALUE_SEPARATOR + moviesPartialCount));
    }
}
