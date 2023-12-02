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
