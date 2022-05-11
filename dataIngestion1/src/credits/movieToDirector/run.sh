#!/bin/bash
javac -classpath `hadoop classpath` MovieToDirectorMapper.java
javac -classpath `hadoop classpath` MovieToDirectorReducer.java
javac -classpath `hadoop classpath`:. MovieToDirectorMappingDriver.java
jar cvf movieToDirector.jar *.class
hadoop jar movieToDirector.jar MovieToDirectorMappingDriver /user/av2783/theMovieProject/tmdb_5000_credits.tsv /user/av2783/theMovieProject/movieToDirector