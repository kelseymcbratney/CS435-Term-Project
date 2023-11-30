package SentimentLauncher;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    // Set up the second map-reduce job to calculate inverse document frequency
    // (IDF)
    // Job job2 = Job.getInstance(conf, "TF-IDF Job - IDF");
    //
    // job2.setJarByClass(TFIDFJob.class);
    // job2.setMapperClass(TFIDFJob.TFIDFMapper.class);
    // job2.setMapOutputKeyClass(Text.class);
    // job2.setMapOutputValueClass(Text.class);
    // job2.setReducerClass(TFIDFJob.TFIDFReducer.class);
    // job2.setOutputKeyClass(Text.class);
    // job2.setOutputValueClass(Text.class);
    //
    // FileInputFormat.addInputPath(job2, new Path(args[1] + "/tf"));
    // FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/tfidf"));
    //
    // job2.waitForCompletion(true);
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
