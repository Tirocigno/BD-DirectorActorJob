package it.unibo.bd1819.utils;

import it.unibo.bd1819.Main;
import it.unibo.bd1819.combiner.FindThreeActorsCombiner;
import it.unibo.bd1819.mapper.*;
import it.unibo.bd1819.reducers.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

import static it.unibo.bd1819.utils.Paths.MAIN_OUTPUT_PATH;

public class JobFactory {

    public static final Path titleBasicsPath = new Path(Paths.TITLE_BASICS_PATH);
    public static final Path titlePrincipalsPath = new Path(Paths.TITLE_PRINCIPALS_PATH);
    public static final Path nameBasicsPath = new Path(Paths.NAME_BASICS_PATH);
    public static final Path outputPath = new Path(MAIN_OUTPUT_PATH);
    public static final Path aggregateDirectorPath = new Path(Paths.AGGREGATED_DIRECTORS_OUTPUT_PATH);
    public static final Path basicprincipalsJoinPath = new Path(Paths.JOIN_TITLE_BASICS_PRINCIPALS_PATH);
    public static final Path directorActorsJoinPath = new Path(Paths.JOIN_ACTORS_DIRECTORS_OUTPUT_PATH);
    public static final Path threeActorsDirectorPath = new Path(Paths.THREE_ACTORS_DIRECTORS_OUTPUT_PATH);
    public static final Path joinDirectorsNamePath = new Path(Paths.JOIN_DIRECTORS_NAME_OUTPUT_PATH);
    public static final Path joinActorsNamePath = new Path(Paths.JOIN_ACTORS_NAME_OUTPUT_PATH);

    /**
     * Create a job to join the movies from title.basics and their directors from title.principals
     * @param conf the job configuration
     * @return a hadoop Job.
     * @throws Exception if something is wrong in the process
     */
    public static Job createDirectorsMovieJoin(final Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);

        deleteOutputFolder(fs, outputPath);
        deleteOutputFolder(fs, basicprincipalsJoinPath);

        Job joinPrincipalBasicJob = Job.getInstance(conf, "Join between title.principals and title.basics");

        joinPrincipalBasicJob.setReducerClass(FindDirectorsMovieJoinReducer.class);

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

    /**
     * Create a job for counting how many films has a director made
     * @param conf the job configuration.
     * @return an hadoop Job.
     * @throws Exception if something is wrong in the process.
     */
    public static Job createAggregatorJob(final Configuration conf) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, aggregateDirectorPath);

        Job aggregationJob = Job.getInstance(conf, "Aggregation job for the directors");

        aggregationJob.setJarByClass(Main.class);
        aggregationJob.setMapperClass(AggregateDirectorsMapper.class);
        aggregationJob.setInputFormatClass(KeyValueTextInputFormat.class);

        aggregationJob.setReducerClass(AggregateDirectorsReducer.class);
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(aggregationJob, basicprincipalsJoinPath);
        FileOutputFormat.setOutputPath(aggregationJob, aggregateDirectorPath);

        return aggregationJob;
    }

    /**
     * Join the director-movies table with the actors from title.principals
     * @param conf the job configuration.
     * @return an Hadoop job
     * @throws Exception if something goes wrong.
     */
    public static Job createDirectorsActorsJoin(final Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, aggregateDirectorPath);

        deleteOutputFolder(fs, directorActorsJoinPath);

        Job joinDirectorsActor = Job.getInstance(conf, "Join between Actors and Directors");

        joinDirectorsActor.setReducerClass(ActorDirectorJoinReducer.class);
        //DEBUG:joinPrincipalBasicJob.setReducerClass(DebugReducer.class);

        joinDirectorsActor.setJarByClass(Main.class);

        joinDirectorsActor.setMapOutputKeyClass(Text.class);
        joinDirectorsActor.setMapOutputValueClass(Text.class);

        joinDirectorsActor.setOutputKeyClass(Text.class);
        joinDirectorsActor.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(joinDirectorsActor, directorActorsJoinPath);

        MultipleInputs.addInputPath(joinDirectorsActor, aggregateDirectorPath,
                KeyValueTextInputFormat.class, FilteredDirectorMovieMapper.class);

        MultipleInputs.addInputPath(joinDirectorsActor, titlePrincipalsPath,
                KeyValueTextInputFormat.class, ActorJoinMapper.class);
        return joinDirectorsActor;
    }

    /**
     * Search the three most frequent actors for each director
     * @param conf the job configuration
     * @return an Hadoop job
     * @throws Exception if something goes wrong
     */
    public static Job createThreeActorDirectorJob(final Configuration conf) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, threeActorsDirectorPath);

        Job threeDirectorsActorJob = Job.getInstance(conf, "Find for each director the three actors");

        threeDirectorsActorJob.setMapperClass(FindThreeActorsMapper.class);
        threeDirectorsActorJob.setReducerClass(FindThreeActorsReducer.class);
        //DEBUG:joinPrincipalBasicJob.setReducerClass(DebugReducer.class);

        threeDirectorsActorJob.setJarByClass(Main.class);

        threeDirectorsActorJob.setMapOutputKeyClass(Text.class);
        threeDirectorsActorJob.setMapOutputValueClass(Text.class);

        threeDirectorsActorJob.setOutputKeyClass(Text.class);
        threeDirectorsActorJob.setOutputValueClass(Text.class);

        threeDirectorsActorJob.setCombinerClass(FindThreeActorsCombiner.class);
        threeDirectorsActorJob.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(threeDirectorsActorJob, directorActorsJoinPath);
        FileOutputFormat.setOutputPath(threeDirectorsActorJob, threeActorsDirectorPath);

        return threeDirectorsActorJob;
    }

    /**
     * Join director ids with their names
     * @param conf the job configuration
     * @return an Hadoop job
     * @throws Exception if something goes wronh.
     */
    public static Job createDirectorsNameJoin(final Configuration conf) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, joinDirectorsNamePath);

        Job joinDirectorsName = Job.getInstance(conf, "Join between Names and Directors");

        joinDirectorsName.setReducerClass(DirectorsNameReducer.class);
        //DEBUG:joinPrincipalBasicJob.setReducerClass(DebugReducer.class);

        joinDirectorsName.setJarByClass(Main.class);


        joinDirectorsName.setOutputKeyClass(Text.class);
        joinDirectorsName.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(joinDirectorsName, joinDirectorsNamePath);

        MultipleInputs.addInputPath(joinDirectorsName, joinActorsNamePath,
                KeyValueTextInputFormat.class, DirectorNameJoinMapper.class);

        MultipleInputs.addInputPath(joinDirectorsName, nameBasicsPath,
                KeyValueTextInputFormat.class, NameJoinerMapper.class);
        return joinDirectorsName;
    }

    /**
     * Join actors id with their names.
     * @param conf the job configuration
     * @return a Hadoop job
     * @throws Exception if something goes wrong.
     */
    public static Job createActorsNameJoin(final Configuration conf) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, joinActorsNamePath);

        Job joinActorsName = Job.getInstance(conf, "Join between Names and Actors");

        joinActorsName.setReducerClass(ActorsNameJoinReducer.class);
        //DEBUG:joinPrincipalBasicJob.setReducerClass(DebugReducer.class);

        joinActorsName.setJarByClass(Main.class);


        joinActorsName.setOutputKeyClass(Text.class);
        joinActorsName.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(joinActorsName, joinActorsNamePath);

        MultipleInputs.addInputPath(joinActorsName, threeActorsDirectorPath,
                KeyValueTextInputFormat.class, ActorNameJoinMapper.class);

        MultipleInputs.addInputPath(joinActorsName, nameBasicsPath,
                KeyValueTextInputFormat.class, NameJoinerMapper.class);
        return joinActorsName;
    }


    /**
     * Sort globally the data emitted by the previous jobs.
     * @param conf the job configuration
     * @return a Hadoop job
     * @throws Exception if something goes wrong.
     */
    public static Job createSortedJob(final Configuration conf) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, outputPath);

        Job sortJob = new Job(conf);

        sortJob.setJarByClass(Main.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);


        sortJob.setMapOutputKeyClass(LongWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setReducerClass(SortReducer.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        FileInputFormat.addInputPath(sortJob, joinDirectorsNamePath);
        FileOutputFormat.setOutputPath(sortJob, outputPath);
        //TODO CHANGE THIS
        sortJob.setNumReduceTasks(1);

        return sortJob;
    }

    public static class DebugReducer
            extends Reducer<LongWritable,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(Text t : values) {
                context.write(new Text(key.toString()), t);
            }
        }
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }
}
