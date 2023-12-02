import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.lit;

public class RandomForestJob {
  public static void main(String[] args) {
    // Create a Spark session
    // Adding Try and catch to handle the erro
    try {
      SparkSession spark = SparkSession.builder()
          .appName("RandomForestExample")
          .config("spark.master", "local")
          .config("spark.executor.memory", "32g")
          .config("spark.driver.memory", "4g")
          .config("spark.memory.offHeap.enabled", true)
          .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
          .getOrCreate();

      // Define the schema for the input data
      StructType schema = DataTypes.createStructType(new StructField[] {
          DataTypes.createStructField("ReviewId", DataTypes.StringType, false),
          DataTypes.createStructField("Rating", DataTypes.DoubleType, false),
          DataTypes.createStructField("Unigram", DataTypes.StringType, false),
          DataTypes.createStructField("TFIDF", DataTypes.DoubleType, false),
          DataTypes.createStructField("Sentiment", DataTypes.StringType, false)
      });

      // Load the data from a file into a DataFrame
      Dataset<Row> data = spark.read().option("delimiter", "\t").schema(schema)
          .csv("/SentimentAnalysis/tfidf/part-r-00000");

      // Convert the "Unigram" column to numeric using StringIndexer
      StringIndexerModel unigramIndexerModel = new StringIndexer()
          .setInputCol("Unigram")
          .setOutputCol("indexedUnigram")
          .fit(data);

      Dataset<Row> indexedData = unigramIndexerModel.transform(data);

      // Convert the "TFIDF" column to numeric using StringIndexer
      StringIndexerModel labelIndexerModel = new StringIndexer()
          .setInputCol("TFIDF")
          .setOutputCol("indexedLabel")
          .fit(indexedData);

      Dataset<Row> labelIndexedData = labelIndexerModel.transform(indexedData);

      // Create a feature vector by combining relevant columns
      VectorAssembler assembler = new VectorAssembler()
          .setInputCols(new String[] { "Rating", "indexedUnigram", "TFIDF" })
          .setOutputCol("features");

      Dataset<Row> assembledData = assembler.transform(labelIndexedData);

      // Split the data into training and test sets
      double[] weights = { 0.8, 0.2 };
      long seed = 42L;
      Dataset<Row>[] splits = assembledData.randomSplit(weights, seed);
      Dataset<Row> trainingData = splits[0];
      Dataset<Row> testData = splits[1];

      // Train a RandomForest model
      RandomForestClassifier rf = new RandomForestClassifier()
          .setLabelCol("indexedLabel")
          .setFeaturesCol("features")
          .setMaxDepth(12)
          .setNumTrees(100)
          .setMaxBins(1300000); // Set maxBins to a value greater than or equal to the number of unique values

      RandomForestClassificationModel model = rf.fit(trainingData);

      // Make predictions on the test data
      Dataset<Row> predictions = model.transform(testData);

      // Evaluate the model
      MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("indexedLabel")
          .setPredictionCol("prediction")
          .setMetricName("accuracy");
      double accuracy = evaluator.evaluate(predictions);

      // Evaluating the model error on training data
      double trainingAccuracy = evaluator.evaluate(model.transform(trainingData));
      double trainingError = 1.0 - trainingAccuracy;

      // Evaluating the model error on test data
      double testingAccuracy = evaluator.evaluate(model.transform(testData));
      double testingError = 1.0 - testingAccuracy;

      // Calculate precision, recall, and F1-score
      MulticlassClassificationEvaluator prfEvaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("indexedLabel")
          .setPredictionCol("prediction");

      double precision = prfEvaluator.setMetricName("weightedPrecision").evaluate(predictions);
      double recall = prfEvaluator.setMetricName("weightedRecall").evaluate(predictions);
      double f1 = prfEvaluator.setMetricName("f1").evaluate(predictions);

      System.out.println("Precision = " + precision);
      System.out.println("Recall = " + recall);
      System.out.println("F1 Score = " + f1);
      System.out.println("Training Error Percentage = " + (trainingError * 100) + "%");
      System.out.println("Testing Error Percentage = " + (testingError * 100) + "%");
      System.out.println("Test Accuracy = " + accuracy);

      // Add precision, recall, and f1-score columns to the DataFrame
      predictions = predictions.withColumn("precision", lit(precision))
          .withColumn("recall", lit(recall))
          .withColumn("f1", lit(f1));

      // Save the predictions with additional metrics to a file in TSV (Tab-Separated
      // Values) format
      predictions.select("ReviewId", "indexedLabel", "prediction", "precision", "recall", "f1")
          .write()
          .option("delimiter", "\t")
          .mode(SaveMode.Overwrite)
          .csv("/SentimentAnalysis/predictions");

      // Stop the Spark session
      spark.stop();
    } catch (Exception e) {
      // Handle exceptions
      System.out.println("An error has been found: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
