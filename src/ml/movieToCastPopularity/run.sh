#!/bin/bash
javac -classpath `hadoop classpath` CastPopularityMapper.java
javac -classpath `hadoop classpath` CastPopularityReducer.java
javac -classpath `hadoop classpath`:. CastPopularityDriver.java
jar cvf castPopularity.jar *.class
hadoop jar castPopularity.jar CastPopularityDriver /user/av2783/theMovieProject/ml/ml_train_new.tsv /user/av2783/theMovieProject/castPopularity