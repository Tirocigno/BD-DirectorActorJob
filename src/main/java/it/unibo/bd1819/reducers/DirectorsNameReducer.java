package it.unibo.bd1819.reducers;

import it.unibo.bd1819.mapper.DirectorNameJoinMapper;
import it.unibo.bd1819.mapper.NameJoinerMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

public class DirectorsNameReducer extends Reducer<Text, Text, Text, Text> {
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
               context.write(new Text(name), buildSortedActorString(directorTuples));
           }
        }

    }

    private String getMoviesDirected(List<String> directorTuples) {
        return directorTuples.get(0).split(CUSTOM_VALUE_SEPARATOR)[2];
    }

    private Text buildSortedActorString(final List<String> directorTuples) {
        String sortedAndAggregatedString = getMoviesDirected(directorTuples);
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
