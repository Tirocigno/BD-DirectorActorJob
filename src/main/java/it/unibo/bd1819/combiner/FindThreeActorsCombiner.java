package it.unibo.bd1819.combiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner for three actors class
 */
public class FindThreeActorsCombiner extends Reducer<Text, Text, Text, Text> {
}
