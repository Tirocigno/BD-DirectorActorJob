package it.unibo.bd1819.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

/**
 * Map the records using the ActorID as Key and inserting a prefix to distinguish the tuple in the reduce phase.
 */
public class ActorNameJoinMapper extends Mapper<Text, Text, Text, Text> {

    public final static String ACTOR_NAME_JOIN_PREFIX = "actrnm-";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(getActorIDAsText(value),
                new Text(ACTOR_NAME_JOIN_PREFIX +buildDirectorStatisticsValue(key, value)));
    }

    /**
     * Retreive the Actor ID and set it as a key.
     * @param value the value of the current pair
     * @return the actorID of the pair.
     */
    private Text getActorIDAsText(final Text value) {
        return new Text(value.toString().split(CUSTOM_VALUE_SEPARATOR)[0]);
    }

    /**
     * Builder for the new value of the key, switch the ActorID with the DirectorID.
     * @param directorID the directorID as text
     * @param statisticsValue the other data to be send as values
     * @return a text in the format directorID, statisticValue
     */
    private String buildDirectorStatisticsValue(final Text directorID, final Text statisticsValue) {
        return  directorID.toString() +
                statisticsValue.toString()
                        .replace(getActorIDAsText(statisticsValue).toString(), "");
    }
}
