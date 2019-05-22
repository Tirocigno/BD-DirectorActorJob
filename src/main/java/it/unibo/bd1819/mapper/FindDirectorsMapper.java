package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;


/**
 * Mapper for the FindDirectors Job
 */
public class FindDirectorsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final static String ROLE = "director";
    private final static String VALUE_SEPARATOR = "\\t";
    private final static String EMPTY_VALUE = "";

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String directorID = filterByRole(value);
        if(!directorID.equals(EMPTY_VALUE)) {
            context.write(new Text(directorID), ONE);
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
        if(values[3].equals(ROLE)) return values[2];
        else return "";
    }
}
