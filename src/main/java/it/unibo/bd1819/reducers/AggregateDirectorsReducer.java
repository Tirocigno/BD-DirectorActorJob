package it.unibo.bd1819.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.Separators.CUSTOM_VALUE_SEPARATOR;

public class AggregateDirectorsReducer extends Reducer<Text, Text,Text, Text> {



    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        List<Text> tmpList = new ArrayList<>();
        for (Text val : values) {
            tmpList.add(val);
        }
        for (Text val : tmpList) {
            int moviesPartialCount = Integer.parseInt(val.toString().split(CUSTOM_VALUE_SEPARATOR)[1]);
            sum += moviesPartialCount;
        }
        for (Text val : tmpList) {
            String newKey = val.toString().split(CUSTOM_VALUE_SEPARATOR)[0];
            context.write(new Text(newKey), new Text(key.toString() + CUSTOM_VALUE_SEPARATOR + sum));
        }

    }
}
