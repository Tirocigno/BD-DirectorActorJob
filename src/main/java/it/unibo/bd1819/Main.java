package it.unibo.bd1819;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;

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
        deleteOutputFolder(fs, aggregateDirectorPath);
        deleteOutputFolder(fs, directorActorsJoinPath);
        deleteOutputFolder(fs, threeActorsDirectorPath);
        deleteOutputFolder(fs, joinDirectorsNamePath);
        jobs.add(createDirectorsMovieJoin(conf));
        jobs.add(createAggregatorJob(conf));
        jobs.add(createDirectorsActorsJoin(conf));
        jobs.add(createThreeActorDirectorJob(conf));
        jobs.add(createDirectorsNameJoin(conf));


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


