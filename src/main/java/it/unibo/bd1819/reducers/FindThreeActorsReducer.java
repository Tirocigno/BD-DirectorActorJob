package it.unibo.bd1819.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * For each director will collect and count all the actors.
 */
public class FindThreeActorsReducer extends Reducer<Text,Text,Text, Text> {

    private static final int CHOOSEN_ACTORS_NUMBER = 3;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        String filmDirected = "";
        Map<String,Integer> actorsDirectorFrequencyMap = new HashMap<>();
        for(Text value : values) {
            String[] decompressedValue = value.toString().split(CUSTOM_VALUE_SEPARATOR);
            filmDirected = decompressedValue[1];
            processActor(actorsDirectorFrequencyMap, decompressedValue[0]);
        }

        for(int i = 0; i < CHOOSEN_ACTORS_NUMBER; i++) {
            context.write(key, new Text(
                 findAndRemoveMostFrequentActor(actorsDirectorFrequencyMap) +
                 CUSTOM_VALUE_SEPARATOR +
                 filmDirected
            ));
        }
    }

    /**
     * Store the frequency of an actorID in the collection.
     * @param actorsMap the collection to store the frequencies for each actor.
     * @param actorID the actorID to process.
     */
    private void processActor(Map<String,Integer> actorsMap, String actorID) {
        if (!actorsMap.containsKey(actorID)) {
            actorsMap.put(actorID, 1);
        } else {
            actorsMap.put(actorID, actorsMap.get(actorID) + 1);
        }
    }

    private String findAndRemoveMostFrequentActor(final Map<String,Integer> actorsMap) {
        int max = 0;
        String choosenActor = "";
        for(Map.Entry<String, Integer> entry : actorsMap.entrySet()) {
            if(entry.getValue() > max) {
                max = entry.getValue();
                choosenActor = entry.getKey();
            }
        }
        actorsMap.remove(choosenActor);
        return choosenActor + CUSTOM_VALUE_SEPARATOR + max;
    }
}
