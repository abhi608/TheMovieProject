#!/bin/bash
javac -classpath `hadoop classpath` MovieToCastMapper.java
javac -classpath `hadoop classpath` MovieToCastReducer.java
javac -classpath `hadoop classpath`:. MovieToCastMappingDriver.java
jar cvf movieToDirToPop.jar *.class
hadoop jar movieToDirToPop.jar MovieToCastMappingDriver /user/av2783/theMovieProject/hive/directorPopularity/movie_award_stats.csv /user/av2783/theMovieProject/directorPopularity