#!/bin/bash
javac -classpath `hadoop classpath` GenreUniqueMapper.java
javac -classpath `hadoop classpath` GenreUniqueReducer.java
javac -classpath `hadoop classpath`:. GenreUniqueDriver.java
jar cvf genreUnique.jar *.class
hadoop jar genreUnique.jar GenreUniqueDriver /user/av2783/theMovieProject/ml/ml_train_new.tsv /user/av2783/theMovieProject/genreUnique