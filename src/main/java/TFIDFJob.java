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
        int totalTerms = 0;
        Map<String, Integer> termFrequencyMap = new HashMap<>();

        while (tokenizer.hasMoreTokens()) {
          String token = tokenizer.nextToken();
          if (!stopWords.contains(token)) {
            totalTerms++;
            termFrequencyMap.put(token, termFrequencyMap.getOrDefault(token, 0) + 1);
          }
        }

        for (Map.Entry<String, Integer> entry : termFrequencyMap.entrySet()) {
          double tf = (double) entry.getValue() / totalTerms;
          context.write(docId, new Text(entry.getKey() + ":" + tf + ":" + totalTerms));
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
      // Initialize a list to store unigram and its corresponding TF and total term
      // count
      List<Triple<String, Double, Integer>> unigramList = new ArrayList<>();
      int totalTerms = 0;

      // Iterate through the values and store unigram, TF, and total term count
      for (Text value : values) {
        String[] parts = value.toString().split(":");
        String unigram = parts[0];
        double tf = Double.parseDouble(parts[1]);
        int termsInDocument = Integer.parseInt(parts[2]);
        totalTerms += termsInDocument;

        unigramList.add(new ImmutableTriple<>(unigram, tf, termsInDocument));
      }

      // Sort the unigram list by TF in descending order
      unigramList.sort(Comparator.comparing(Triple::getMiddle).reversed());

      // Build the result string with unigram, TF, rank, and total term count
      StringBuilder resultBuilder = new StringBuilder();
      int rank = 1;
      for (Triple<String, Double, Integer> triple : unigramList) {
        String unigram = triple.getLeft();
        double tf = triple.getMiddle();
        resultBuilder.append(unigram).append(":").append(tf).append(", Rank:").append(rank++).append(", ");
      }

      // Set the result text
      result.set(resultBuilder.toString());

      // Emit the result for the key (rating)
      context.write(key, result);
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
