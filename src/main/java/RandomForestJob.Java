import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

public class RandomForestExample {

    public static void main(String[] args) {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("RandomForestExample")
                .config("spark.master", "local")
                .getOrCreate();

        // Define the schema for the input data
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ReviewId", DataTypes.StringType, false),
                DataTypes.createStructField("Rating", DataTypes.DoubleType, false),
                DataTypes.createStructField("Unigram", DataTypes.StringType, false),
                DataTypes.createStructField("TFIDF", DataTypes.DoubleType, false),
                DataTypes.createStructField("Sentiment", DataTypes.StringType, false)
        });

        // Load the data from a file into a DataFrame
        Dataset<Row> data = spark.read().option("delimiter","\t").schema(schema).text("./sentiment-tfidf-100000-sample.txt");
        
        //schema(schema).tab-separated().csv("./sentiment-tfidf-100000-sample.txt");

        // Convert the "label/TFIDF" column to numeric using StringIndexer
        StringIndexer labelIndexer = new StringIndexer()
                .setInputCol("TFIDF")
                .setOutputCol("indexedLabel")
                .fit(data);
        Dataset<Row> indexedData = labelIndexer.transform(data);

        // Create a feature vector by combining relevant columns
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"Rating", "Unigram" ,"TFIDF"})
                .setOutputCol("TFIDF");
        Dataset<Row> assembledData = assembler.transform(indexedData);

        // Split the data into training and test sets
        double[] weights = {0.8, 0.2};
        long seed = 42L;
        JavaRDD<Row>[] splits = assembledData.javaRDD().randomSplit(weights, seed);
        Dataset<Row> trainingData = spark.createDataFrame(Arrays.asList(splits[0]), schema);
        Dataset<Row> testData = spark.createDataFrame(Arrays.asList(splits[1]), schema);

        // Train a RandomForest model
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("TFIDF")
                .setNumTrees(10);

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

        // Stop the Spark session
        spark.stop();
    }
}
