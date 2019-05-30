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

        jobs.add(createDirectorsMovieJoin(conf));
        jobs.add(createAggregatorJob(conf));
        jobs.add(createDirectorsActorsJoin(conf));
        jobs.add(createThreeActorDirectorJob(conf));
        jobs.add(createActorsNameJoin(conf));
        jobs.add(createDirectorsNameJoin(conf));

        for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }
}


