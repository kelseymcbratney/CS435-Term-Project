# CS435-Term-Project
## Sentiment Analysis of Amazon Review using Hadoop and Spark

This project inputs data from the [Amazon Review Dataset](https://nijianmo.github.io/amazon/index.html) and outputs the TF and TFIDF of each word in the dataset. The TF and TFIDF are then used to train a Random Forest Model. The accuracy of the classifiers are then outputted.

## Run Sentiment Analysis

### Full Dataset
./compile-and-run.sh

### Small Dataset
./compile-and-run-sample.sh

## Output TF
hadoop fs -cat /SentimentAnalysis/tf/*

## Output TFIDF
hadoop fs -cat /SentimentAnalysis/tfidf/*

## Run Spark Machine Learning on Dataset
./compile-and-run-spark.sh

The result accuracy will be printed on the screen.

## Results

Dataset: Sample - 14,429 entries.
Max Depth, Max Trees
 
10 - 100
Precision = 0.7341422191058127
Recall = 0.6769722814498934
F1 Score = 0.6717607259538676
Training Error Percentage = 27.645286267757207%
Testing Error Percentage = 32.30277185501066%
Test Accuracy = 0.6769722814498934
 
8 - 200
Precision = 0.6120851348604399
Recall = 0.6019900497512438
F1 Score = 0.5761587332121166
Training Error Percentage = 35.21308652604391%
Testing Error Percentage = 39.80099502487562%
Test Accuracy = 0.6019900497512438
 
12 - 100
Precision = 0.7747338977155214
Recall = 0.7377398720682303
F1 Score = 0.7303161435158251
Training Error Percentage = 21.179509255273356%
Testing Error Percentage = 26.226012793176967%
Test Accuracy = 0.7377398720682303
