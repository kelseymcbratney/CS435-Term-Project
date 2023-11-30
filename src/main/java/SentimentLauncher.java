package SentimentLauncher;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SentimentLauncher {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 2) {
      System.err.println("Usage: SentimentLauncher <input path> <output path>");
      System.exit(-1);
    }

    Job job1 = Job.getInstance(conf, "TF Job");

    job1.setJarByClass(TFIDFJob.class);

    // Set up the first map-reduce job to calculate term frequency (TF)
    job1.setMapperClass(TFIDFJob.TFTokenizer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setReducerClass(TFIDFJob.SumReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/tf"));

    job1.waitForCompletion(true);

    Counter totalRecords = job1.getCounters().findCounter(Counters.TOTAL_RECORDS);

    Job job2 = Job.getInstance(conf, "TF Job");

    job2.setJarByClass(TFIDFJob.class);
    job2.getConfiguration().set("total_records", Long.toString(counter.getValue()));

    // Set up the first map-reduce job to calculate term frequency (TF)
    job2.setMapperClass(TFIDFJob.TFTokenizer.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setReducerClass(TFIDFJob.SumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/tf"));

    job2.waitForCompletion(true);

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
