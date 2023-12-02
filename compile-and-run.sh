#!/bin/bash

git pull

mkdir SentimentLauncher

rm SentimentLauncher/*.class
rm SentimentLauncher.jar

hadoop com.sun.tools.javac.Main src/main/java/SentimentLauncher.java src/main/java/TFIDFJob.java

mv src/main/java/*.class SentimentLauncher/

jar cfm SentimentLauncher.jar Manifest.txt SentimentLauncher/

hadoop fs -rm -r /SentimentAnalysis/tf
hadoop fs -rm -r /SentimentAnalysis/tfidf

hadoop jar SentimentLauncher.jar /SentimentAnalysis/home-and-kitchen.json /SentimentAnalysis/ /SentimentAnalysis/stopwords.txt
