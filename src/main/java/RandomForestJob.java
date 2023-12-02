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

public class RandomForestJob {

  public static void main(String[] args) {
    // Create a Spark session
    SparkSession spark = SparkSession.builder()
        .appName("RandomForestExample")
        .config("spark.master", "local")
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
        .setNumTrees(10)
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
    System.out.println("Test Accuracy = " + accuracy);
    System.out.println("Test Accuracy = " + accuracy);
    System.out.println("Test Accuracy = " + accuracy);
    System.out.println("Test Accuracy = " + accuracy);

    // Stop the Spark session
    spark.stop();
  }
}
