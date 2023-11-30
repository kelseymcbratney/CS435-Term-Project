#!/bin/bash

git pull

rm SentimentLauncher/*.class
rm SentimentLauncher.jar

hadoop com.sun.tools.javac.Main *.java

mv *.class SentimentLauncher/

jar cfm SentimentLauncher.jar Manifest.txt SentimentLauncher/

hadoop fs -rm -r /SentimentAnalysis/tf
hadoop fs -rm -r /SentimentAnalysis/idf
hadoop fs -rm -r /SentimentAnalysis/tfidf
