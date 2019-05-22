package it.unibo.bd1819;

import it.unibo.bd1819.mapper.FindDirectorsMapper;
import it.unibo.bd1819.mocktest.WordCount;
import it.unibo.bd1819.reducers.FindDirectorsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Find Directors");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Main.class);
        job.setMapperClass(FindDirectorsMapper.class);
        job.setReducerClass(FindDirectorsReducer.class);

        //Set output class corresponding to the one specified in reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
