package SentimentLauncher;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SentimentLauncher {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 2) {
      System.err.println("Usage: SentimentLauncher <input path> <output path>");
      System.exit(-1);
    }

    Job job = Job.getInstance(conf, "TF-IDF Job");

    job.setJarByClass(TFIDFJob.class);

    // Set up the first map-reduce job to calculate term frequency (TF)
    job.setMapperClass(TFIDFJob.TFTokenizer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(TFIDFJob.SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tf"));

    job.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "Combine Job");
    job.setJarByClass(TFIDFJob.class);

    // Set the mapper and reducer classes
    job2.setMapperClass(TFIDFJob.CombineMapper.class);
    job2.setReducerClass(TFIDFJob.CombineReducer.class);

    // Set the output key and value classes
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    // Set the input and output paths
    FileInputFormat.addInputPath(job, new Path(args[1] + "/tf")); // Replace with your actual input path
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/combinedtf")); // Replace with your desired output path

    // job.waitForCompletion(true);
    //
    // // Set up the third map-reduce job to calculate TF-IDF
    // job = Job.getInstance(conf, "TF-IDF Job - TF-IDF");
    //
    // job.setJarByClass(TFIDFJob.class);
    // job.setMapperClass(TFIDFJob.TFMapper.class);
    // job.setMapOutputKeyClass(Text.class);
    // job.setMapOutputValueClass(Text.class);
    // job.setReducerClass(TFIDFJob.TFIDFReducer.class);
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(Text.class);
    //
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    // FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tfidf"));
    //
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
