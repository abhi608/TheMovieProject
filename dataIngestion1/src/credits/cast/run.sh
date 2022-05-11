#!/bin/bash
javac -classpath `hadoop classpath` CastMapper.java
javac -classpath `hadoop classpath` CastReducer.java
javac -classpath `hadoop classpath`:. CastMappingDriver.java
jar cvf cast.jar *.class
hadoop jar cast.jar CastMappingDriver /user/av2783/theMovieProject/tmdb_5000_credits.tsv /user/av2783/theMovieProject/cast