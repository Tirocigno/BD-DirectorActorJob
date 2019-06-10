package it.unibo.bd1819.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

public class SortReducer extends Reducer<IntWritable, Text, Text, Text> {
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        for(Text value: values) {
            context.write(extractNewKey(value), extractNewValue(value, key));
        }
    }

    private Text extractNewKey(final Text oldValue) {
        return new Text("Director:" + oldValue.toString().split(CUSTOM_VALUE_SEPARATOR)[0]);
    }

    private Text extractNewValue(final Text oldValue, final IntWritable moviesDirected) {
        final String directorID = oldValue.toString().split(CUSTOM_VALUE_SEPARATOR)[0] + CUSTOM_VALUE_SEPARATOR;
        return new Text("Movies Directed: " + (-moviesDirected.get()) + ", Most Frequently actors: " +
                oldValue.toString().replace(directorID,""));
    }
}
