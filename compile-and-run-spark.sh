#!/bin/bash

mvn clean package

spark-submit --class RandomForestJob --master spark://salem:30160 target/RandomForestJob-1.0-SNAPSHOT.jar
