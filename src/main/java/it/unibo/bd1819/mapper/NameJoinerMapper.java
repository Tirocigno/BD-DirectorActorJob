package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;
import static it.unibo.bd1819.utils.Separators.VALUE_SEPARATOR;


public class NameJoinerMapper extends Mapper<Text, Text, Text, Text> {

    public final static String NAME_JOIN_PREFIX = "nmjnprx-";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, buildNewValue(value));
    }

    /**
     * Build the new value to be send as output to the reduce phase
     * @param value the input value.
     * @return a Text containing the new formatted value
     */
    private Text buildNewValue(final Text value) {
        return new Text(NAME_JOIN_PREFIX +
                extractName(value));
    }

    /**
     * Extract the name of an actor/director from the table
     * @param value the input value of the mapper
     * @return a string containing the name as a value
     */
    private String extractName(final Text value) {
        return value.toString().split(VALUE_SEPARATOR)[0];
    }
}
