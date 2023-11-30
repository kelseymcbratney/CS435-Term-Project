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
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

public class TFIDFJob {
  public static class TFTokenizer extends Mapper<LongWritable, Text, Text, Text> {
    private final Text word = new Text();
    private final Text docId = new Text();
    private final Text rating = new Text();
    private final ObjectMapper mapper = new ObjectMapper();
    private static int counter = 0; // Counter for generating unique IDs
    private static Set<String> stopWords;

    static {
      // Initialize stop words set
      stopWords = new HashSet<>();
      stopWords.add("each");
      stopWords.add("every");
      stopWords.add("her");
      stopWords.add("his");
      stopWords.add("its");
      stopWords.add("my");
      stopWords.add("no");
      stopWords.add("our");
      stopWords.add("some");
      stopWords.add("that");
      stopWords.add("the");
      stopWords.add("their");
      stopWords.add("this");
      // Add coordinating conjunctions
      stopWords.add("and");
      stopWords.add("but");
      stopWords.add("or");
      stopWords.add("yet");
      stopWords.add("for");
      stopWords.add("nor");
      stopWords.add("so");
      // Add prepositions
      stopWords.add("a");
      stopWords.add("as");
      stopWords.add("aboard");
      stopWords.add("about");
      stopWords.add("above");
      stopWords.add("across");
      stopWords.add("after");
      stopWords.add("against");
      stopWords.add("along");
      stopWords.add("around");
      stopWords.add("at");
      stopWords.add("before");
      stopWords.add("behind");
      stopWords.add("below");
      stopWords.add("beneath");
      stopWords.add("beside");
      stopWords.add("between");
      stopWords.add("beyond");
      stopWords.add("but");
      stopWords.add("by");
      stopWords.add("down");
      stopWords.add("during");
      stopWords.add("except");
      stopWords.add("following");
      stopWords.add("for");
      stopWords.add("from");
      stopWords.add("in");
      stopWords.add("inside");
      stopWords.add("into");
      stopWords.add("like");
      stopWords.add("minus");
      stopWords.add("minus");
      stopWords.add("near");
      stopWords.add("next");
      stopWords.add("of");
      stopWords.add("off");
      stopWords.add("on");
      stopWords.add("onto");
      stopWords.add("onto");
      stopWords.add("opposite");
      stopWords.add("out");
      stopWords.add("outside");
      stopWords.add("over");
      stopWords.add("past");
      stopWords.add("plus");
      stopWords.add("round");
      stopWords.add("since");
      stopWords.add("since");
      stopWords.add("than");
      stopWords.add("through");
      stopWords.add("to");
      stopWords.add("toward");
      stopWords.add("under");
      stopWords.add("underneath");
      stopWords.add("unlike");
      stopWords.add("until");
      stopWords.add("up");
      stopWords.add("upon");
      stopWords.add("very");
      stopWords.add("with");
      stopWords.add("without");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      try {
        // Parse JSON
        JsonNode jsonNode = mapper.readTree(value.toString());

        // Extract values
        String overall = jsonNode.get("overall").asText();
        String reviewText = jsonNode.get("reviewText").asText().replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();

        // Generate a unique ID using the counter
        int uniqueId = counter++;

        // Emit the values with unigrams, flipping word and docId
        StringTokenizer tokenizer = new StringTokenizer(reviewText);
        while (tokenizer.hasMoreTokens()) {
          String token = tokenizer.nextToken();
          if (!stopWords.contains(token)) {
            docId.set(Integer.toString(uniqueId));
            word.set(token);
            rating.set(overall);
            context.write(docId, new Text(rating.toString() + ", " + "1" + ", " + word.toString()));
          }
        }
      } catch (Exception e) {
        // Handle parsing errors
        System.err.println("Error parsing JSON: " + e.getMessage());
      }
    }
  }

  public static class SumReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();

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

      // Build the result string with unigram frequencies and the overall rating
      StringBuilder resultBuilder = new StringBuilder();
      for (Map.Entry<String, Integer> entry : unigramCountMap.entrySet()) {
        resultBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
      }
      // Append the overall rating
      resultBuilder.append("overall:").append(rating);

      // Set the result text
      result.set(resultBuilder.toString());

      // Emit the result for the key (docId)
      context.write(new Text(rating), result);
    }
  }

  public static class CombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text rating = new Text();
    private final IntWritable count = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split(":|,"); // Split by colon or comma
      if (parts.length >= 2) {
        rating.set(parts[0].trim());
        count.set(Integer.parseInt(parts[1].trim().replaceAll("[^0-9]", "")));
        context.write(rating, count);
      }
    }
  }

  public static class CombineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

}
