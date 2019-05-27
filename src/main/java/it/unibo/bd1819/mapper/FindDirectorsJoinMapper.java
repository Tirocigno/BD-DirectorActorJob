package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.VALUE_SEPARATOR;


/**
 * Mapper for the FindDirectors Job
 */
public class FindDirectorsJoinMapper extends Mapper<Text, Text, Text, Text> {

    private final static String ROLE = "director";
    private final static String EMPTY_VALUE = "";
    public final static String PRINCIPALS_JOIN_PREFIX = "prcjnprx-";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String directorID = filterByRole(value);
        if(!directorID.equals(EMPTY_VALUE)) {
            context.write(key, new Text(PRINCIPALS_JOIN_PREFIX + directorID));
        }
    }


    /**
     * Filter the lines by role and return the ID of the directors
     * @param text the text to filter
     * @return a string containing the director ID
     */
    private String filterByRole(final Text text) {
        String line = text.toString();
        String[] values = line.split(VALUE_SEPARATOR);
        if(values[2].equals(ROLE)) return values[1];
        else return EMPTY_VALUE;
    }
}
