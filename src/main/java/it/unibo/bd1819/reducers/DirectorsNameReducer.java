package it.unibo.bd1819.reducers;

import it.unibo.bd1819.mapper.DirectorNameJoinMapper;
import it.unibo.bd1819.mapper.NameJoinerMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            for(String directorData: directorTuples) {
                context.write(new Text(name), new Text(directorData));
            }
        }

    }
}
