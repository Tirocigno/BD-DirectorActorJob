package it.unibo.bd1819.combiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Combiner for three actors class.
 */
public class FindThreeActorsCombiner extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        String filmDirected = "";
        String actorFrequency = "";
        Map<String, Integer> actorsDirectorFrequencyMap = new HashMap<>();
        for (Text value : values) {
            String[] decompressedValue = value.toString().split(CUSTOM_VALUE_SEPARATOR);
            filmDirected = decompressedValue[1];
            actorFrequency = decompressedValue[2];
            processActor(actorsDirectorFrequencyMap, decompressedValue[0], actorFrequency);
        }

        for (Map.Entry<String, Integer> tuple : actorsDirectorFrequencyMap.entrySet()) {
            context.write(key, new Text(tuple.getKey() + CUSTOM_VALUE_SEPARATOR +
                    filmDirected + CUSTOM_VALUE_SEPARATOR +
                    tuple.getValue()));
        }
    }

    /**
     * Store the frequency of an actorID in the collection.
     * @param actorsMap the collection to store the frequencies for each actor.
     * @param actorID the actorID to process.
     * @param actorFrequency the frequency of the actor
     */
    private void processActor(Map<String,Integer> actorsMap, String actorID, String actorFrequency) {
        if (!actorsMap.containsKey(actorID)) {
            actorsMap.put(actorID, Integer.parseInt(actorFrequency));
        } else {
            actorsMap.put(actorID, actorsMap.get(actorID) + Integer.parseInt(actorFrequency));
        }
    }



}
