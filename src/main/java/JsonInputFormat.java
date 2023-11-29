package SentimentLauncher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new JsonRecordReader();
  }

  public static class JsonRecordReader extends RecordReader<LongWritable, Text> {

    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private static long counter = 0;
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      // No initialization needed for this example
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!processed) {
        key.set(counter++); // Using a counter for unique keys

        try {
          // Read the content of the split into a single Text value
          // This is just a basic example; adjust it based on your actual needs
          byte[] buffer = new byte[4096]; // Choose an appropriate buffer size
          int bytesRead;

          while ((bytesRead = inStream.read(buffer)) > 0) {
            content.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
          }

          String jsonString = content.toString();

          // Parse the JSON and extract the required fields
          ObjectMapper objectMapper = new ObjectMapper();
          JsonNode jsonNode = objectMapper.readTree(jsonString);

          String overall = jsonNode.get("overall").asText();
          String reviewText = jsonNode.get("reviewText").asText();

          // Transform the reviewText: remove non-alphanumeric characters and convert to
          // lowercase
          reviewText = reviewText.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();

          // Construct the final value with the required fields
          value.set(overall + "," + reviewText);

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
