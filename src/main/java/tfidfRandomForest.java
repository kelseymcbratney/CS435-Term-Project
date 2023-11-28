import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import weka.core.tokenizers.NGramTokenizer;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.classifiers.Evaluation;
import weka.core.Utils;
import weka.core.stopwords.WordsFromFile;

import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

//imports for graph
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import javax.swing.JFrame;



public class RandomForestExample {

    public static void main(String[] args) {
        try {
            // Step 1: Load CSV file
            String csvFilePath = "your_data.csv";
            CSVParser csvParser = new CSVParser(new FileReader(csvFilePath), CSVFormat.DEFAULT.withHeader());

            // Step 2: Prepare data for Weka
            Instances instances = createWekaInstances(csvParser.getRecords());

            // Step 3: Split data into train and test sets (80:20 ratio)
            Instances[] splitData = splitData(instances, 80);

            Instances trainingData = splitData[0];
            Instances testingData = splitData[1];

            // Step 4: Apply TF-IDF transformation
            StringToWordVector filter = new StringToWordVector();
            filter.setInputFormat(trainingData);
            filter.setLowerCaseTokens(true);
            Instances filteredTrainingData = Filter.useFilter(trainingData, filter);
            Instances filteredTestingData = Filter.useFilter(testingData, filter);
            NGramTokenizer tokenizer = new NGramTokenizer();
            tokenizer.setNGramMinSize(1);
            tokenizer.setNGramMaxSize(2);
            tokenizer.setDelimiters("\\W");
            filter.setTokenizer(tokenizer);

            // Use a file with stop words or specify them manually
            WordsFromFile stopwordHandler = new WordsFromFile();
            stopwordHandler.setStopwords(new File("path_to_stopwords.txt"));
            filter.setStopwordsHandler(stopwordHandler);

            filter.setOutputWordCounts(true);


            // Step 5: Train Random Forest model
            RandomForest randomForest = new RandomForest();
            randomForest.setNumTrees(100); // Example: Setting the number of trees. You can set more parameters based on your requirements
            FilteredClassifier classifier = new FilteredClassifier();
            classifier.setFilter(filter);
            classifier.setClassifier(randomForest);
            classifier.buildClassifier(filteredTrainingData);

            // Step 6: Evaluate the model
            evaluateModel(classifier, filteredTestingData);

        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Instances createWekaInstances(List<CSVRecord> records) throws Exception {
        // Implement this method based on your CSV file structure
        // You need to convert your CSV data into Weka Instances
        // For example, if your CSV has text data and a class label, create attributes accordingly
        // Return the Instances object
        //return null;

        Attribute textAttribute = new Attribute("text", (FastVector) null);
        Attribute classAttribute = new Attribute("class");

        // Create the attribute vector
        FastVector attributes = new FastVector();
        attributes.addElement(textAttribute);
        attributes.addElement(classAttribute);

        // Create Instances object
        Instances instances = new Instances("TextClassification", attributes, records.size());

        // Iterate through CSV records and add instances
        for (CSVRecord record : records) {
            // Assuming "text" and "class" are column names in your CSV
            //String text = record.get("text");
            // Assuming 'reviewText' is your text column
            String text = preprocessText(record.get("reviewText"));
            //int rating = Integer.parseInt(record.get("rating"));
            String classLabel = record.get("class");

            // Create a new instance
            DenseInstance instance = new DenseInstance(2);
            instance.setValue(textAttribute, text);
            instance.setValue(classAttribute, classLabel);
            // Add the instance to the dataset
            instances.add(instance);
        }
        instances.setClassIndex(instances.numAttributes()-1);
        return instances;
    }

    private static String preprocessText(String text) {
        // Lowercase conversion
        text = text.toLowerCase();
        // Remove special characters and digits
        text = text.replaceAll("[^a-zA-Z\\s]", "");
        // Replace multiple spaces with a single space
        text = text.replaceAll("\\s+", " ");

        return text.trim(); // Trim leading and trailing spaces
    }

    private static Instances[] splitData(Instances data, int percentage) {
        // Implement this method to split the data into training and testing sets
        // The 'percentage' parameter determines the size of the training set (e.g., 80%)
        // Return an array containing training and testing sets
        //return null;
        if (percentage < 0 || percentage > 100) {
            throw new IllegalArgumentException("Percentage should be between 0 and 100");
        }
        // Calculate the number of instances for training and testing
        int numInstances = data.numInstances();
        int trainSize = (int) Math.round(numInstances * percentage / 100.0);
        int testSize = numInstances - trainSize;
        // Create training and testing sets
        Instances trainData = new Instances(data, 0, trainSize);
        Instances testData = new Instances(data, trainSize, testSize);
        Instances[] result = new Instances[2];
        result[0] = trainData;
        result[1] = testData;
        return result;
    }

    private static void evaluateModel(FilteredClassifier classifier, Instances testingData) throws Exception {
        // Implement this method to evaluate the performance of the trained model on the testing set
        // Print relevant metrics such as accuracy, precision, recall, etc.

        // Set up the evaluation
        Evaluation evaluation = new Evaluation(testingData);
        
        // Data for the graph
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        dataset.addValue(evaluation.correct(), "Predictions", "Correct");
        dataset.addValue(evaluation.incorrect(), "Predictions", "Incorrect");

        // Evaluate the model
        evaluation.evaluateModel(classifier, testingData);

        // Print evaluation results
        System.out.println("Evaluation Results:");
        //correct prediction
        System.out.println("Correctly classified instances: " + (int) evaluation.correct());
        //incorrect prediction
        System.out.println("Incorrectly classified instances: " + (int) evaluation.incorrect());
        System.out.println("Accuracy: " + evaluation.pctCorrect() + "%");
        System.out.println("Precision: " + evaluation.weightedPrecision());
        System.out.println("Recall: " + evaluation.weightedRecall());
        System.out.println("F1 Score: " + evaluation.weightedFMeasure());
        System.out.println("Confusion Matrix:");
        System.out.println(evaluation.toMatrixString());

        //Make Graph, correct graph vs incorrect graph 
        // Create the chart
        JFreeChart barChart = ChartFactory.createBarChart(
            "Model Prediction Results",
            "Category",
            "Number of Instances",
            dataset
        );

        // Display the chart
        ChartPanel chartPanel = new ChartPanel(barChart);
        JFrame frame = new JFrame();
        frame.setContentPane(chartPanel);
        frame.setSize(800, 600);
        frame.setVisible(true);

    }
}

/*
Updates 11/28
Added:
 -The createWekaInstances method to handle CSV data. This includes preprocessing the text and handling different types of attributes
 -In the main method, configure the StringToWordVector filter with appropriate settings
 -Tune the RandomForest parameters
 -Improved error handling in your main method



String reviewerID
int asin
String reviewerName
int vote
String Size
String Color
String reviewText
double overall
string summary
int unixReviewTime
string reviewTime

reviewerID,asin,reviewerName,vote,Size,Color,reviewText,overall,summary,unixReviewTime,reviewTime
{AUI6WTTT0QZYS,5120053084,Abbey,2,Large,Charcoal,"I now have 4 of the 5 available colors of this shirt...",5.0,"Comfy, flattering, discreet--highly recommended!",1514764800,"01 1, 2018"}
{A2SUAM1J3GNN3B,0000013714,J. McDonald,5,Hardcover,, "I bought this. Great purchase though!",5.0,"Heavenly Highway Hymns",1252800000,"09 13, 2009"}
*/