# CS435-Term-Project

## Compile
./compile.sh

## Run Sentiment Analysis
hadoop jar SentimentLauncher.jar /SentimentAnalysis/home-and-kitchen-sample.json /SentimentAnalysis/ /SentimentAnalysis/stopwords.txt

## Gather TF
hadoop fs -cat /SentimentAnalysis/tf/*

## Gather TFIDF
hadoop fs -cat /SentimentAnalysis/tfidf/*
