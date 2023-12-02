# CS435-Term-Project
## Sentiment Analysis of Amazon Review Dataset

## Run Sentiment Analysis

### Full Dataset
./compile-and-run.sh

### Small Dataset
./compile-and-run-sample.sh

## Gather TF
hadoop fs -cat /SentimentAnalysis/tf/*

## Gather TFIDF
hadoop fs -cat /SentimentAnalysis/tfidf/*

## Run Spark Machine Learning on Dataset
./run-spark.sh 
