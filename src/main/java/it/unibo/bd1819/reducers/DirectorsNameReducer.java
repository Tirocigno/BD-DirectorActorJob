package it.unibo.bd1819.reducers;

import it.unibo.bd1819.mapper.DirectorNameJoinMapper;
import it.unibo.bd1819.mapper.NameJoinerMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

public class DirectorsNameReducer extends Reducer<Text, Text, IntWritable, Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        List<String> directorTuples = new ArrayList<>();
        List<String> nameTuples = new ArrayList<>();

        for(Text value : values) {
            if(value.toString().contains(DirectorNameJoinMapper.DIRECTOR_JOIN_PREFIX)) {
                directorTuples.add(value.toString().replace(DirectorNameJoinMapper.DIRECTOR_JOIN_PREFIX, ""));
            }

            if(value.toString().contains(NameJoinerMapper.NAME_JOIN_PREFIX)) {
                nameTuples.add(value.toString().replace(NameJoinerMapper.NAME_JOIN_PREFIX, ""));
            }
        }

        for(String name : nameTuples) {
           if(!directorTuples.isEmpty()) {
               context.write(new IntWritable(- this.getMoviesDirected(directorTuples)),
                       buildSortedActorString(name,directorTuples));
           }
        }

    }

    /**
     * Get Movies directed by the director
     * @param directorTuples the list of directorTuples
     * @return an integer containing all the collaboration
     */
    private int getMoviesDirected(List<String> directorTuples) {
        return Integer.parseInt(directorTuples.get(0).split(CUSTOM_VALUE_SEPARATOR)[2]);
    }

    /**
     * Aggregate three actor tuples into one sorted by the collaboration number
     * @param directorID the director ID key of the reducer
     * @param directorTuples the input tuples corresponding to a Director
     * @return
     */
    private Text buildSortedActorString(final String directorID, final List<String> directorTuples) {
        String sortedAndAggregatedString = directorID;
        while (! directorTuples.isEmpty()) {
            int maxCollaboration = 0;
            String selectedDirectorTuple = "";
            for(String tuple : directorTuples) {
                try {
                int currentCollaboration = Integer.parseInt(tuple.split(CUSTOM_VALUE_SEPARATOR)[1]);
                if ( currentCollaboration > maxCollaboration ) {
                    maxCollaboration = currentCollaboration;
                    selectedDirectorTuple = tuple;
                }
            } catch (Exception e) {
                return new Text("EXCEPTION CAUSED BY " + tuple);
            }}
            sortedAndAggregatedString += CUSTOM_VALUE_SEPARATOR + selectedDirectorTuple.split(CUSTOM_VALUE_SEPARATOR)[0];
            sortedAndAggregatedString += "(" + maxCollaboration + ")";
            directorTuples.remove(selectedDirectorTuple);
        }

        return  new Text(sortedAndAggregatedString);
    }


}
