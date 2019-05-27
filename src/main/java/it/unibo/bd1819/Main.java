package it.unibo.bd1819;

import it.unibo.bd1819.mapper.AggregateDirectorsMapper;
import it.unibo.bd1819.mapper.FindDirectorsJoinMapper;
import it.unibo.bd1819.mapper.FindMovieJoinMapper;
import it.unibo.bd1819.mapper.SortMapper;
import it.unibo.bd1819.reducers.AggregateDirectorsReducer;
import it.unibo.bd1819.reducers.FindDirectorsMovieJoinReducer;
import it.unibo.bd1819.reducers.SortReducer;
import it.unibo.bd1819.utils.Paths;
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
import java.util.ArrayList;
import java.util.List;

import static it.unibo.bd1819.utils.JobFactory.*;

public class Main {


    public static void main(String[] args) throws Exception {

        List<Job> jobs = new ArrayList<>();
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new Configuration());

        deleteOutputFolder(fs, outputPath);
        deleteOutputFolder(fs, basicprincipalsJoinPath);
        deleteOutputFolder(fs, sortPath);
        jobs.add(createDirectorsMovieJoin(conf));
        jobs.add(createAggregatorJob(conf));


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


