package it.unibo.bd1819;

import it.unibo.bd1819.utils.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Main {
    private static final Path titleBasicsPath = new Path(Paths.TITLE_BASICS_PATH);
    private static final Path titlePrincipalsPath = new Path(Paths.TITLE_PRINCIPALS_PATH);
    private static final Path nameBasicsPath = new Path(Paths.NAME_BASICS_PATH);
    private static final Path outputPath = new Path(Paths.GENERIC_OUTPUT_PATH);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job joinTitlesTable = Job.getInstance(conf, "Join between title.principals and title.basics");

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        /*job.setJarByClass(Main.class);
        job.setMapperClass(FindDirectorsMapper.class);
        job.setReducerClass(FindDirectorsMovieJoinReducer.class);

        //Set output class corresponding to the one specified in reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    }
}
