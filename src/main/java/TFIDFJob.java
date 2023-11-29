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

  public static class TFMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text word = new Text();
    private final Text docId = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Tokenize the document content and emit word counts
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      String overall = itr.nextToken().trim();
      String reviewText = itr.nextToken().trim();
      String uniqueDocId = key.toString();

      StringTokenizer tokenizer = new StringTokenizer(reviewText);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        docId.set(uniqueDocId); // Using the uniqueDocId for each word
        // Emitting key-value pair with the same docId for each word
        context.write(docId, word);
      }
    }
  }

  public static class SumReducer extends Reducer<Text, Text, Text, Text> {

    private final static Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Create a StringBuilder to store unigrams and count
      StringBuilder unigramBuilder = new StringBuilder();

      // Iterate through values and count the unigrams
      int count = 0;
      for (Text value : values) {
        if (count > 0) {
          unigramBuilder.append(", ");
        }
        unigramBuilder.append(value.toString().split(":")[0]);
        count++;
      }

      // Emit the result with unigrams and count
      result.set("Unigrams: " + unigramBuilder.toString() + ", Count: " + count);
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
