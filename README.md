# CS435-Term-Project

## Compile
./compile.sh

## Run Sentiment Analysis
hadoop jar SentimentLauncher.jar /SentimentAnalysis/home-and-kitchen.json /SentimentAnalysis

## Gather TFIDF
hadoop fs -cat /SentimentAnalysis/tfidf/*
