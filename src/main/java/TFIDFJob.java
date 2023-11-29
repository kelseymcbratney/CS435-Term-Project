package SentimentLauncher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.Map;
import java.util.HashMap;

public class TFIDFJob {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private final static Text reviewerID = new Text();
    private final static Text info = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // The key is not used in this example, as the input format provides the value
      // as a whole JSON document
      // The value is the JSON document as a Text object

      // Use a JSON parsing library to extract fields from the JSON document
      // Assuming you have a JSON library (like Jackson or Gson) in your classpath

      String jsonText = value.toString();

      // Parse JSON and extract reviewerID, overall, and reviewText
      // Adjust this based on your actual JSON structure and the library you are using
      ObjectMapper objectMapper = new ObjectMapper(); // Example using Jackson
      JsonNode jsonNode = objectMapper.readTree(jsonText);

      String reviewerID = jsonNode.get("reviewerID").asText();
      int overall = jsonNode.get("overall").asInt();
      String reviewText = jsonNode.get("reviewText").asText();

      // Do further processing as needed

      // Emitting key-value pair
      context.write(new Text(reviewerID), new Text(overall + "," + reviewText));
    }

  }

  public class TFMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static Text word = new Text();
    private final static Text docId = new Text();
    private long counter = 0;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Tokenize the document content and emit word counts
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      String overall = itr.nextToken().trim();
      String reviewText = itr.nextToken().trim();

      // Assuming you want to tokenize reviewText, you may need to adjust this based
      // on your specific requirements
      StringTokenizer tokenizer = new StringTokenizer(reviewText);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        docId.set("ID_" + counter++); // Using a unique key
        context.write(word, new Text(docId + ":1"));
      }
    }
  }

  public static class IDFMapper extends Mapper<Text, Text, Text, Text> {

    private final static Text word = new Text();
    private final static Text docIdCount = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      // Emit word and document count
      word.set(key.toString());
      docIdCount.set(value.toString());
      context.write(word, docIdCount);
    }
  }

  public static class SumReducer extends Reducer<Text, Text, Text, Text> {

    private final static Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Calculate the sum of word counts or document counts based on the context
      // Emit the result
      int sum = 0;
      for (Text value : values) {
        sum += Integer.parseInt(value.toString().split(":")[1]);
      }
      result.set(key.toString() + ":" + sum);
      context.write(key, result);
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
