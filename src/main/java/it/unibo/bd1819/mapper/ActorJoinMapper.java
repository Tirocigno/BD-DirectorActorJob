package it.unibo.bd1819.mapper;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.VALUE_SEPARATOR;

/**
 * This mapper prepare the data of actor by movies to be joined by the director counterpart.
 */
public class ActorJoinMapper extends Mapper<Text, Text, Text, Text> {
    private final static String ROLE = "actor";
    private final static String FEMALE_ROLE = "actress";
    private final static String EMPTY_VALUE = "";
    public final static String ACTOR_JOIN_PREFIX = "actr-";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String actorID = filterByRole(value);
        if(!actorID.equals(EMPTY_VALUE)) {
            context.write(key, new Text(ACTOR_JOIN_PREFIX + actorID));
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
        if(values[2].equals(ROLE) || values[2].equals(FEMALE_ROLE)) return values[1];
        else return EMPTY_VALUE;
    }
}
