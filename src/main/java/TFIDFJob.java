package SentimentLauncher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;

public class TFIDFJob {
  public static class TFTokenizer extends Mapper<LongWritable, Text, Text, Text> {
    private final Text word = new Text();
    private final Text docId = new Text();
    private final Text rating = new Text();
    private final ObjectMapper mapper = new ObjectMapper();
    private static int counter = 0; // Counter for generating unique IDs

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      try {
        // Parse JSON
        JsonNode jsonNode = mapper.readTree(value.toString());

        // Extract values
        String overall = jsonNode.get("overall").asText();
        String reviewText = jsonNode.get("reviewText").asText().replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();

        // Generate a unique ID using the counter
        int uniqueId = counter++;

        // Your logic here to process 'overall' and 'reviewText' as needed

        // Emit the values with unigrams, flipping word and docId
        StringTokenizer tokenizer = new StringTokenizer(reviewText);
        while (tokenizer.hasMoreTokens()) {
          docId.set(Integer.toString(uniqueId));
          word.set(tokenizer.nextToken());
          rating.set(overall);
          context.write(docId, new Text(rating.toString() + ", " + "1" + ", " + word.toString()));
        }

      } catch (Exception e) {
        // Handle parsing errors
        System.err.println("Error parsing JSON: " + e.getMessage());
      }
    }
  }

  public static class SumReducer extends Reducer<Text, Text, Text, Text> {
    private final static Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      return;
    }
  }

  public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

    private final static Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Calculate TF-IDF and emit the result
      int totalDocuments = 0;
      int documentsWithWord = 0;
      Map<String, Integer> wordCounts = new HashMap<>();

      // Count the total number of documents and documents containing the word
      for (Text value : values) {
        totalDocuments++;
        String[] parts = value.toString().split(":");
        String documentId = parts[0];
        int count = Integer.parseInt(parts[1]);
        wordCounts.put(documentId, count);
        if (count > 0) {
          documentsWithWord++;
        }
      }

      // Calculate TF-IDF and emit the result for each document
      for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
        String documentId = entry.getKey();
        int termFrequency = entry.getValue();
        double tf = (double) termFrequency / wordCounts.size();
        double idf = Math.log((double) totalDocuments / documentsWithWord);
        double tfidf = tf * idf;

        result.set(key.toString() + ":" + tfidf);
        context.write(new Text(documentId), result);
      }
    }
  }
}
