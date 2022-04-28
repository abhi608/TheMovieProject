#!/bin/bash
javac -classpath `hadoop classpath` MovieToCastMapper.java
javac -classpath `hadoop classpath` MovieToCastReducer.java
javac -classpath `hadoop classpath`:. MovieToCastMappingDriver.java
jar cvf movieToCast.jar *.class
hadoop jar movieToCast.jar MovieToCastMappingDriver /user/av2783/theMovieProject/tmdb_5000_credits.tsv /user/av2783/theMovieProject/movieToCast