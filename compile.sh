#!/bin/bash

git pull

rm SentimentLauncher/*.class
rm SentimentLauncher.jar

hadoop com.sun.tools.javac.Main *.java

mv SentimentLauncher*.class SentimentLauncher/

jar cf SentimentLauncher.jar SentimentLauncher/

hadoop fs -rm -r /SentimentAnalysis/temp
hadoop fs -rm -r /SentimentAnalysis/idf
hadoop fs -rm -r /SentimentAnalysis/tfidf
