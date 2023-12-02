#!/bin/bash

git pull

rm build/SentimentLauncher/*.class
rm build/SentimentLauncher.jar

hadoop com.sun.tools.javac.Main /src/main.java/SentimentLauncher.java /src/main/java/TFIDFJob.java

mv /src/main.java/*.class build/SentimentLauncher/

jar cfm SentimentLauncher.jar Manifest.txt build/SentimentLauncher/

hadoop fs -rm -r /SentimentAnalysis/tf
hadoop fs -rm -r /SentimentAnalysis/tfidf

hadoop jar SentimentLauncher.jar /SentimentAnalysis/home-and-kitchen-sample.json /SentimentAnalysis/ /SentimentAnalysis/stopwords.txt
