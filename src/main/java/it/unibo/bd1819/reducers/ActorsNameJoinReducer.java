package it.unibo.bd1819.reducers;

import it.unibo.bd1819.mapper.ActorNameJoinMapper;
import it.unibo.bd1819.mapper.NameJoinerMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;


public class ActorsNameJoinReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        List<String> actorTuples = new ArrayList<>();
        List<String> nameTuples = new ArrayList<>();

        for(Text value : values) {
            if(value.toString().contains(ActorNameJoinMapper.ACTOR_NAME_JOIN_PREFIX)) {
                actorTuples.add(value.toString().replace(ActorNameJoinMapper.ACTOR_NAME_JOIN_PREFIX, ""));
            }

            if(value.toString().contains(NameJoinerMapper.NAME_JOIN_PREFIX)) {
                nameTuples.add(value.toString().replace(NameJoinerMapper.NAME_JOIN_PREFIX, ""));
            }
        }

        for(String name : nameTuples) {
            for(String actorData: actorTuples) {
                context.write(extractDirectorID(actorData), buildTransformedActorValue(actorData, name));
            }
        }

    }

    /**
     * Extract the directorID from the actorTuple.
     * @param actorTuple the data correlated to an actor
     * @return a Text containing the directorID that will be used as identifier.
     */
    private Text extractDirectorID(final String actorTuple) {
        return new Text(actorTuple.split(CUSTOM_VALUE_SEPARATOR)[0]);
    }

    /**
     * Pack the actorName with the remaining value of the tuple.
     * @param actorTuple the string containing all the actorData
     * @param actorName the string containing the actorName
     * @return a Text containing the data in the format actorName, actorData.
     */
    private Text buildTransformedActorValue(final String actorTuple, final String actorName) {
        return new Text(actorTuple.replace(extractDirectorID(actorTuple).toString(), actorName));
    }
}
