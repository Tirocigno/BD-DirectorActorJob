package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This mapper prepare the data of the director filtered by movies to be joined with the actor counterpart.
 */
public class FilteredDirectorMovieMapper extends Mapper<Text, Text, Text, Text> {
    private final static String ROLE = "actor";
    private final static String EMPTY_VALUE = "";
    public final static String FILTERED_DIRECTORS_JOIN_PREFIX = "fltrdirct-";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
       context.write(key, new Text(FILTERED_DIRECTORS_JOIN_PREFIX + value.toString()));
    }
}
