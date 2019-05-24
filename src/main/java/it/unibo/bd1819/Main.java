package it.unibo.bd1819;

import it.unibo.bd1819.mapper.AggregateDirectorsMapper;
import it.unibo.bd1819.mapper.FindDirectorsMapper;
import it.unibo.bd1819.mapper.FindMovieJoinMapper;
import it.unibo.bd1819.reducers.AggregateDirectorsReducer;
import it.unibo.bd1819.reducers.FindDirectorsMovieJoinReducer;
import it.unibo.bd1819.utils.JoinJob;
import it.unibo.bd1819.utils.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final Path titleBasicsPath = new Path(Paths.TITLE_BASICS_PATH);
    private static final Path titlePrincipalsPath = new Path(Paths.TITLE_PRINCIPALS_PATH);
    private static final Path nameBasicsPath = new Path(Paths.NAME_BASICS_PATH);
    private static final Path outputPath = new Path(Paths.GENERIC_OUTPUT_PATH);
    private static final Path basicprincipalsJoinPath = new Path(Paths.JOIN_TITLE_BASICS_PRINCIPALS_PATH);

    public static void main(String[] args) throws Exception {

        List<Job> jobs = new ArrayList<>();

        Configuration conf = new Configuration();
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Join between title.principals and title.basics");

        FileSystem fs = FileSystem.get(new Configuration());

        deleteOutputFolder(fs, outputPath);

        deleteOutputFolder(fs, basicprincipalsJoinPath);
        MultipleInputs.addInputPath(joinPrincipalBasicJob, titleBasicsPath,
                KeyValueTextInputFormat.class, FindMovieJoinMapper.class);
        MultipleInputs.addInputPath(joinPrincipalBasicJob,
                titlePrincipalsPath, KeyValueTextInputFormat.class, FindDirectorsMapper.class);

        joinPrincipalBasicJob.setReducerClass(FindDirectorsMovieJoinReducer.class);

        joinPrincipalBasicJob.setJarByClass(Main.class);
        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(joinPrincipalBasicJob, basicprincipalsJoinPath);

        jobs.add(joinPrincipalBasicJob);

        Job aggregationJob = Job.getInstance(conf, "Aggregation job for the directors");

        aggregationJob.setJarByClass(Main.class);
        aggregationJob.setMapperClass(AggregateDirectorsMapper.class);

        aggregationJob.setReducerClass(AggregateDirectorsReducer.class);
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(aggregationJob, basicprincipalsJoinPath);
        FileOutputFormat.setOutputPath(aggregationJob, outputPath);

        jobs.add(aggregationJob);

        for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }
}
