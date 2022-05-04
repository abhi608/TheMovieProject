#!/bin/bash
javac -classpath `hadoop classpath` DirectorPopularityMapper.java
javac -classpath `hadoop classpath` DirectorPopularityReducer.java
javac -classpath `hadoop classpath`:. DirectorPopularityDriver.java
jar cvf directorPopularity.jar *.class
hadoop jar directorPopularity.jar DirectorPopularityDriver /user/av2783/theMovieProject/ml/ml_train_new.tsv /user/av2783/theMovieProject/directorPopularity