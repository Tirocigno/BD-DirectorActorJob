package it.unibo.bd1819.combiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Combiner for a partial aggregation of the Movie count for each director
 */
public class AggregateDirectorsCombiner extends Reducer<Text, Text, Text, Text> {

    public static final String COMBINER_SUFFIX= "COMBINEDRECORD";
    private final Set<String> partialMoviesDirected = new TreeSet<>();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
       this.combine(key, values, context);

    }

    /**
     * For each value corresponding to a specified key, sum the movies directed by a single director
     * @param key the DirectorID
     * @param values a iterable containing the values associated to a specific director
     * @param context the hadoop context.
     * @throws IOException
     * @throws InterruptedException
     */
    private void combine(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text val : values) {
            int moviesPartialCount = Integer.parseInt(val.toString().split(CUSTOM_VALUE_SEPARATOR)[1]);
            partialMoviesDirected.add(val.toString().split(CUSTOM_VALUE_SEPARATOR)[0]);
            sum += moviesPartialCount;
        }
        for (String newKey : partialMoviesDirected) {
            context.write(new Text(newKey),
                    new Text(key.toString() +
                            CUSTOM_VALUE_SEPARATOR +
                            sum +
                            CUSTOM_VALUE_SEPARATOR +
                            COMBINER_SUFFIX));
        }
    }


}
