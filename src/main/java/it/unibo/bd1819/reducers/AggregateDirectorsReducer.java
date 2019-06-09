package it.unibo.bd1819.reducers;

import it.unibo.bd1819.combiner.AggregateDirectorsCombiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Reducer for the count of movies directed for each director
 */
public class AggregateDirectorsReducer extends Reducer<Text, Text,Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        Set<String> tmpSet = new HashSet<>();
        for (Text val : values) {
            String movieID = val.toString().split(CUSTOM_VALUE_SEPARATOR)[0];
            if(!val.toString().contains(AggregateDirectorsCombiner.COMBINER_SUFFIX) ||
            ! tmpSet.contains(movieID)) {
                int moviesPartialCount = Integer.parseInt(val.toString().split(CUSTOM_VALUE_SEPARATOR)[1]);
                tmpSet.add(movieID);
                sum += moviesPartialCount;
            }
        }
        for (String newKey : tmpSet) {
            context.write(new Text(newKey), new Text(key.toString() + CUSTOM_VALUE_SEPARATOR + sum));
        }

    }
}
