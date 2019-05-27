package it.unibo.bd1819.utils;

import it.unibo.bd1819.Main;
import it.unibo.bd1819.mapper.*;
import it.unibo.bd1819.reducers.ActorDirectorJoinReducer;
import it.unibo.bd1819.reducers.AggregateDirectorsReducer;
import it.unibo.bd1819.reducers.FindDirectorsMovieJoinReducer;
import it.unibo.bd1819.reducers.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobFactory {

    public static final Path titleBasicsPath = new Path(Paths.TITLE_BASICS_PATH);
    public static final Path titlePrincipalsPath = new Path(Paths.TITLE_PRINCIPALS_PATH);
    public static final Path nameBasicsPath = new Path(Paths.NAME_BASICS_PATH);
    public static final Path outputPath = new Path(Paths.MAIN_OUTPUT_PATH);
    public static final Path sortPath = new Path(Paths.SORTED_OUTPUT_PATH);
    public static final Path basicprincipalsJoinPath = new Path(Paths.JOIN_TITLE_BASICS_PRINCIPALS_PATH);

    public static Job createDirectorsMovieJoin(final Configuration conf) throws Exception {
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Join between title.principals and title.basics");

        joinPrincipalBasicJob.setReducerClass(FindDirectorsMovieJoinReducer.class);
        //DEBUG:joinPrincipalBasicJob.setReducerClass(DebugReducer.class);

        joinPrincipalBasicJob.setJarByClass(Main.class);

        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(Text.class);

        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(joinPrincipalBasicJob, basicprincipalsJoinPath);

        MultipleInputs.addInputPath(joinPrincipalBasicJob, titleBasicsPath,
                KeyValueTextInputFormat.class, FindMovieJoinMapper.class);

        MultipleInputs.addInputPath(joinPrincipalBasicJob,
                titlePrincipalsPath, KeyValueTextInputFormat.class, FindDirectorsJoinMapper.class);
        return joinPrincipalBasicJob;
    }

    public static Job createAggregatorJob(final Configuration conf) throws Exception {

        Job aggregationJob = Job.getInstance(conf, "Aggregation job for the directors");

        aggregationJob.setJarByClass(Main.class);
        aggregationJob.setMapperClass(AggregateDirectorsMapper.class);
        aggregationJob.setInputFormatClass(KeyValueTextInputFormat.class);

        aggregationJob.setReducerClass(AggregateDirectorsReducer.class);
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(aggregationJob, basicprincipalsJoinPath);
        FileOutputFormat.setOutputPath(aggregationJob, sortPath);

        return aggregationJob;
    }

    public static Job createDirectorsActorsJoin(final Configuration conf) throws Exception {
        Job joinDirectorsActor = Job.getInstance(conf, "Join between Actors and Directors");

        joinDirectorsActor.setReducerClass(ActorDirectorJoinReducer.class);
        //DEBUG:joinPrincipalBasicJob.setReducerClass(DebugReducer.class);

        joinDirectorsActor.setJarByClass(Main.class);

        joinDirectorsActor.setMapOutputKeyClass(Text.class);
        joinDirectorsActor.setMapOutputValueClass(Text.class);

        joinDirectorsActor.setOutputKeyClass(Text.class);
        joinDirectorsActor.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(joinDirectorsActor, outputPath);

        MultipleInputs.addInputPath(joinDirectorsActor, sortPath,
                KeyValueTextInputFormat.class, FilteredDirectorMovieMapper.class);

        MultipleInputs.addInputPath(joinDirectorsActor, titlePrincipalsPath,
                KeyValueTextInputFormat.class, ActorJoinMapper.class);
        return joinDirectorsActor;
    }



    public static Job createSortedJob(final Configuration conf, final Path inputPath,
                                       final Path outputPath) throws Exception {

        Job sortJob = Job.getInstance(conf, "Generic sort");

        sortJob.setJarByClass(Main.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);

        sortJob.setMapOutputKeyClass(IntWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setReducerClass(SortReducer.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(sortJob, inputPath);
        FileOutputFormat.setOutputPath(sortJob, outputPath);

        return sortJob;
    }

    public static class DebugReducer
            extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(Text t : values) {
                context.write(key, t);
            }
        }
    }
}
