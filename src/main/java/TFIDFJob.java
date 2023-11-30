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
import java.util.Arrays;
import java.util.Comparator;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import java.io.File;
import java.io.InputStreamReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TFIDFJob {
  private static Counter totalRecordsCounter;

  public static enum Counters {
    TOTAL_RECORDS
  }

  public static Counter getTotalRecordsCounter() {
    return totalRecordsCounter;
  }

  public static class TFTokenizer extends Mapper<LongWritable, Text, Text, Text> {
    private final Text RatingUnigramCount = new Text();
    private final ObjectMapper mapper = new ObjectMapper();
    private static Set<String> stopWords;

    public void setup(Context context) throws IOException, InterruptedException {
      URI[] cacheFiles = context.getCacheFiles();

      if (cacheFiles != null && cacheFiles.length > 0) {
        try {
          System.err.println("Reading stop words from: " + cacheFiles[0].toString());

          String line = "";

          // Create a FileSystem object and pass the
          // configuration object in it. The FileSystem
          // is an abstract base class for a fairly generic
          // filesystem. All user code that may potentially
          // use the Hadoop Distributed File System should
          // be written to use a FileSystem object.
          FileSystem fs = FileSystem.get(context.getConfiguration());
          Path getFilePath = new Path(cacheFiles[0].toString());

          // We open the file using FileSystem object,
          // convert the input byte stream to character
          // streams using InputStreamReader and wrap it
          // in BufferedReader to make it more efficient
          BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

          while ((line = reader.readLine()) != null) {
            String[] words = line.split(" ");

            for (int i = 0; i < words.length; i++) {
              // add the words to ArrayList
              stopWords.add(words[i]);
            }
          }
        }

        catch (Exception e) {
          e.printStackTrace(); // Print the stack trace for detailed error information
          System.out.println("Unable to read the File");
          System.exit(1);
        }

      }

    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.TOTAL_RECORDS).increment(1);
      try {
        // Parse JSON
        JsonNode jsonNode = mapper.readTree(value.toString());

        // Extract values
        String rating = jsonNode.get("overall").asText();
        String reviewerID = jsonNode.get("reviewerID").asText();
        long unixReviewTime = jsonNode.get("unixReviewTime").asLong();

        // Concatenate reviewerID and unixReviewTime to create a uniqueID
        String uniqueID = reviewerID + "_" + unixReviewTime;

        // Emit each unigram and its TF value separately
        StringTokenizer tokenizer = new StringTokenizer(
            jsonNode.get("reviewText").asText().replaceAll("[^A-Za-z0-9 ]", "").toLowerCase());
        while (tokenizer.hasMoreTokens()) {
          String unigram = tokenizer.nextToken();
          if (!stopWords.contains(unigram)) {
            // Create a new instance of Text for each emission
            Text RatingUnigramCount = new Text();

            // Emit the unique ID (docID), rating, and unigram
            RatingUnigramCount.set(rating + "\t" + unigram + "\t" + "1");
            context.write(new Text(uniqueID), RatingUnigramCount);
          }
        }
      } catch (Exception e) {
        // Handle parsing errors
        System.err.println("Error parsing JSON: " + e.getMessage());
      }
    }

  }

  public static class TFReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();
    private String rating;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Initialize a map to store the count of each unigram
      Map<String, Integer> unigramCountMap = new HashMap<>();

      // Iterate through the values and count the occurrences of each unigram
      int totalWords = 0; // Total words in the document for calculating TF

      for (Text value : values) {
        String[] parts = value.toString().split("\t");

        // Check if the value has the expected format
        if (parts.length >= 3) {
          String unigram = parts[1]; // Assuming the unigram is at index 1
          int count = Integer.parseInt(parts[2]); // Assuming the count is at index 2
          rating = parts[0]; // Assuming the rating is at index 0

          // Update the count in the map
          unigramCountMap.put(unigram, unigramCountMap.getOrDefault(unigram, 0) + count);

          // Accumulate total words
          totalWords += count;
        } else {
          // Log a warning or handle the unexpected format
          System.err.println("Unexpected format: " + value.toString());
        }
      }

      for (String unigram : unigramCountMap.keySet()) {
        // Calculate TF
        double tf = (double) unigramCountMap.get(unigram) / (double) totalWords;
        // Emit key in the format: docID:rating:unigram
        result.set(rating + "\t" + unigram + "\t" + tf);
        context.write(key, result);
      }

    }
  }

  public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(new Text(key.toString()), new Text(value));
    }
  }

  public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();
    private String rating;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      long totalReviewCount = context.getConfiguration().getLong("total_records", 0L);
      int documentFrequency = 0;

      // Collect values into a list
      List<String> valueList = new ArrayList<>();
      for (Text value : values) {
        documentFrequency++;
        valueList.add(value.toString());
        String[] parts = value.toString().split("\t");

      }

      for (String value : valueList) {
        String[] parts = value.toString().split("\t");
        if (parts.length >= 4) {
          String reviewID = parts[0];
          rating = parts[1];
          String unigram = parts[2];
          String tfString = parts[3];

          try {
            double tf = Double.parseDouble(tfString);
            double idf = Math.log10((double) totalReviewCount / (double) documentFrequency);
            double tfidf = tf * idf;

            // Determine sentiment value based on rating
            String sentimentValue;
            if ("5.0".equals(rating) || "4.0".equals(rating)) {
              sentimentValue = "Positive";
            } else if ("3.0".equals(rating)) {
              sentimentValue = "Neutral";
            } else {
              sentimentValue = "Negative";
            }

            // Create a new Text object for each emission
            Text resultText = new Text(rating + "\t" + unigram + "\t" + tfidf + "\t" + sentimentValue);
            context.write(new Text(reviewID), resultText);
          } catch (NumberFormatException e) {
            // Handle the case where tfString is not a valid double
            System.err.println("Error parsing TF value: " + tfString);
          }
        }
      }

    }
  }

}
