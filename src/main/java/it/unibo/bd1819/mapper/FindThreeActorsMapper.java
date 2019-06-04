package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_KEY_SEPARATOR;
import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Mapper for finding the three most frequent actors for each director.
 */
public class FindThreeActorsMapper extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        context.write(buildNewKey(key), buildNewValue(key, value));
    }

    /**
     * Extract the director ID from the previous pass.
     * @param oldKey the old key, formed by directorID:actorID
     * @return the directorID as Text
     */
    private Text buildNewKey(final Text oldKey) {
        return new Text(oldKey.toString().split(CUSTOM_KEY_SEPARATOR)[0]);
    }

    /**
     * Create the new value for the sort
     * @param oldKey the old key, formed by directorID:actorID
     * @param oldValue the movies that a director has directed
     * @return a string formed by actorID,oldValue
     */
    private Text buildNewValue(final Text oldKey, final Text oldValue) {
        return new Text(oldKey.toString().split(CUSTOM_KEY_SEPARATOR)[1] + CUSTOM_VALUE_SEPARATOR +
                oldValue.toString() +
                CUSTOM_VALUE_SEPARATOR +
                1);
    }
}
