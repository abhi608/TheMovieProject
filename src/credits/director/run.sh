#!/bin/bash
javac -classpath `hadoop classpath` DirectorMapper.java
javac -classpath `hadoop classpath` DirectorReducer.java
javac -classpath `hadoop classpath`:. DirectorMappingDriver.java
jar cvf director.jar *.class
hadoop jar director.jar DirectorMappingDriver /user/av2783/theMovieProject/tmdb_5000_credits.tsv /user/av2783/theMovieProject/director