package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

import static it.unibo.bd1819.utils.Paths.VALUE_SEPARATOR;

/**
 * Mapper that find all movies inside title.basics file
 */
public class FindMovieJoinMapper extends  Mapper<Object, Text, Text, Text> {

    private static final String CATEGORY = "movie";
    private static final String EMPTY_VALUE = "";
    public static final String MOVIES_JOIN_PREFIX = "mvsjnprfx";


    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String movieID = filterByCategory(value);
        if(!movieID.equals(EMPTY_VALUE)) {
            context.write(new Text(MOVIES_JOIN_PREFIX + movieID), new Text(EMPTY_VALUE));
        }
    }


    /**
     * Filter the lines by category and returns the movie id and
     * @param text the text to filter
     * @return a string containing the director ID
     */
    private String filterByCategory(final Text text) {

        String line = text.toString();
        String[] values = line.split(VALUE_SEPARATOR);
        if(values[1].equals(CATEGORY)) return values[0];
        else return EMPTY_VALUE;
    }
}
