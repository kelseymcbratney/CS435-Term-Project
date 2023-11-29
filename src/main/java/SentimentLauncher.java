import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentLauncher {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TF-IDF Job");

    job.setJarByClass(TFIDFJob.class);
    job.setInputFormatClass(JsonInputFormat.class); // Custom input format for JSON (you need to implement this)

    // Set up the first map-reduce job to calculate term frequency (TF)
    job.setMapperClass(TFIDFJob.TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(TFIDFJob.SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"));

    job.waitForCompletion(true);

    // Set up the second map-reduce job to calculate inverse document frequency
    // (IDF)
    job = Job.getInstance(conf, "TF-IDF Job - IDF");

    job.setJarByClass(TFIDFJob.class);
    job.setMapperClass(TFIDFJob.IDFMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(TFIDFJob.SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[1] + "/temp"));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/idf"));

    job.waitForCompletion(true);

    // Set up the third map-reduce job to calculate TF-IDF
    job = Job.getInstance(conf, "TF-IDF Job - TF-IDF");

    job.setJarByClass(TFIDFJob.class);
    job.setMapperClass(TFIDFJob.TFMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(TFIDFJob.TFIDFReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tfidf"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
