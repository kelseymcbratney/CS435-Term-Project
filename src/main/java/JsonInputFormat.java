package SentimentLauncher;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new JsonRecordReader();
  }

  public static class JsonRecordReader extends RecordReader<LongWritable, Text> {

    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      // No initialization needed for this example
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!processed) {
        key.set(0); // Use a constant key since we're reading the whole file as one split
        value.set(""); // Initialize an empty value

        try {
          // Read the entire JSON file into a single Text value
          String jsonString = ""; // Read your JSON file into this string

          // Parse the JSON and extract the required fields
          ObjectMapper objectMapper = new ObjectMapper();
          JsonNode jsonNode = objectMapper.readTree(jsonString);

          int overall = jsonNode.get("overall").asInt();
          String reviewerID = jsonNode.get("reviewerID").asText();
          String reviewText = jsonNode.get("reviewText").asText();

          // Construct the final value with the required fields
          value.set(overall + "," + reviewerID + "," + reviewText);

        } catch (Exception e) {
          e.printStackTrace();
        }

        processed = true;
        return true;
      }

      return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
      // No cleanup needed for this example
    }
  }
}
