package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

public class SortMapper extends Mapper<Text, Text, LongWritable, Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
       context.write(extractNumberOfFilmsDirected(value), generateNewKey(key, value));
    }

    private LongWritable extractNumberOfFilmsDirected(final Text directorTuple) {
        Long filmDirected = Long.parseLong(directorTuple.toString().split(CUSTOM_VALUE_SEPARATOR)[0]);
        return new LongWritable(filmDirected);
    }

    private Text generateNewKey(final Text directorName, final Text directorAttributes) {
        String filmDirected = directorAttributes.toString().split(CUSTOM_VALUE_SEPARATOR)[0];
        return new Text(directorAttributes.toString()
                .replace(filmDirected, directorName.toString()));
    }

}
