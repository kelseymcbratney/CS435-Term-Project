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

    private final Text result = new Text();
    private String rating;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Initialize a map to store the count of each unigram
      Map<String, Integer> unigramCountMap = new HashMap<>();

      // Iterate through the values and count the occurrences of each unigram
      for (Text value : values) {
        String[] parts = value.toString().split(", ");
        String unigram = parts[2]; // Assuming the unigram is at index 2
        int count = Integer.parseInt(parts[1]); // Assuming the count is at index 1
        rating = parts[0];

        // Update the count in the map
        unigramCountMap.put(unigram, unigramCountMap.getOrDefault(unigram, 0) + count);
      }

      // Build the result string with unigram frequencies
      StringBuilder resultBuilder = new StringBuilder();
      for (Map.Entry<String, Integer> entry : unigramCountMap.entrySet()) {
        resultBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
      }

      // Set the result text
      result.set(rating + ", " + resultBuilder.toString());

      // Emit the result for the key (docId)
      context.write(key, result);
    }
  }

  public static class TFIDFMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private final static Text result = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      System.out.println("Key type: " + key.getClass().getName());
      System.out.println("Value type: " + value.getClass().getName());

      // Continue with your processing
      context.write(key, value);
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
        String[] parts = value.toString().split(",");
        int count = Integer.parseInt(parts[1]);
        wordCounts.put(key.toString(), count);
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

        result.set(documentId + ":" + tfidf);
        context.write(new Text(documentId), result);
      }
    }
  }
}
