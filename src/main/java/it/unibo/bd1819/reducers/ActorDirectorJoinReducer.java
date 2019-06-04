package it.unibo.bd1819.reducers;

import it.unibo.bd1819.mapper.ActorJoinMapper;
import it.unibo.bd1819.mapper.FilteredDirectorMovieMapper;
import it.unibo.bd1819.mapper.FindDirectorsJoinMapper;
import it.unibo.bd1819.mapper.FindMovieJoinMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.Separators.CUSTOM_KEY_SEPARATOR;
import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

public class ActorDirectorJoinReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Reduce the join between directors and actors.
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        List<String> directorRecords = new ArrayList<>();
        List<String> actorRecord = new ArrayList<>();

        for(Text record: values) {
            if(record.toString().contains(FilteredDirectorMovieMapper.FILTERED_DIRECTORS_JOIN_PREFIX)) {
                directorRecords.add(record.toString()
                        .replace(FilteredDirectorMovieMapper.FILTERED_DIRECTORS_JOIN_PREFIX, ""));
            }
            if(record.toString().contains(ActorJoinMapper.ACTOR_JOIN_PREFIX)) {
                actorRecord.add(record.toString().replace(ActorJoinMapper.ACTOR_JOIN_PREFIX, ""));
            }
        }

        for(String directorData : directorRecords) {
            for(String actorData: actorRecord) {
                context.write(buildDirectorActorKey(directorData, actorData),
                        buildDirectorActorValue(directorData));
            }
        }
    }

    /**
     * Build the new key for the tuple
     * @param directorData the data of a director(directorID, totalMovies directed)
     * @param actorData the actorID
     * @return a Text writable to the context
     */
    private Text buildDirectorActorKey(String directorData, String actorData) {
        String directorID = directorData.split(CUSTOM_VALUE_SEPARATOR)[0];
        return new Text(directorID + CUSTOM_KEY_SEPARATOR + actorData);
    }

    /**
     * Build the new value for the tuple
     * @param directorData the data of a director(directorID, totalMovies directed)
     * @return a Text writable to the context
     */
    private Text buildDirectorActorValue(String directorData) {
        String moviesDirected = directorData.split(CUSTOM_VALUE_SEPARATOR)[1];
        return new Text(moviesDirected);
    }

}
