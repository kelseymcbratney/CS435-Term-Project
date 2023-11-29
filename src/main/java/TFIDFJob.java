import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TFIDFJob {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private final static Text word = new Text();
    private final static Text docId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Parse JSON and extract document ID and content
      // Assuming your JSON structure is {"id": "doc_id", "content":
      // "document_content"}
      // You may need to adjust this based on your actual JSON structure
      // Use a JSON parsing library for better handling
      // For simplicity, this example assumes a simple text extraction
      String[] parts = value.toString().split("\"id\":");
      docId.set(parts[1].split(",")[0].replaceAll("\"", "").trim());
      word.set(parts[2].split("\"content\":")[1].split("}")[0].replaceAll("[^a-zA-Z ]", "").toLowerCase().trim());
      context.write(docId, word);
    }
  }

  public static class TFMapper extends Mapper<Text, Text, Text, Text> {

    private final static Text word = new Text();
    private final static Text docId = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      // Tokenize the document content and emit word counts
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        docId.set(key.toString());
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
      // You'll need to implement your TF-IDF calculation logic here
      // This is a simple example, and you may need to adapt it based on your specific
      // requirements
      // TF-IDF = (TF / total words in the document) * log(total documents / documents
      // containing the word)
      // For simplicity, this example just concatenates the input values as a string
      StringBuilder sb = new StringBuilder();
      for (Text value : values) {
        sb.append(value.toString()).append(",");
      }
      result.set(sb.toString());
      context.write(key, result);
    }
  }
}
