package it.unibo.bd1819.reducers;

import it.unibo.bd1819.mapper.FindDirectorsJoinMapper;
import it.unibo.bd1819.mapper.FindMovieJoinMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.Paths.VALUE_SEPARATOR;


/**
 * Reducer for the FindDirectorsJob.
 */
public class FindDirectorsMovieJoinReducer extends Reducer<Text, Text,Text, IntWritable> {

    private final static String EMPTY_STRING ="";

    /**
     * Collect all the values corresponding to a specific key and for each director
     * print the number of movies he directed.
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        List<String> principalsDatasetRecords = new ArrayList<>();
        int moviesDirected = 0;

        for(Text val : values) {
            if (val.toString().contains(FindDirectorsJoinMapper.PRINCIPALS_JOIN_PREFIX)){
                principalsDatasetRecords.add(val.toString()
                        .replace(FindDirectorsJoinMapper.PRINCIPALS_JOIN_PREFIX, EMPTY_STRING));
            }

            if(val.toString().contains(FindMovieJoinMapper.MOVIES_JOIN_PREFIX)) {
                moviesDirected++;
            }

        }

        for(String directorID : principalsDatasetRecords) {
            if(moviesDirected > 0) {
                context.write(new Text(directorID), new IntWritable(moviesDirected));
            }
        }

    }
}
