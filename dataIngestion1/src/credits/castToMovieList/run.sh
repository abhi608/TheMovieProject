#!/bin/bash
javac -classpath `hadoop classpath` CastToMovieMapper.java
javac -classpath `hadoop classpath` CastToMovieReducer.java
javac -classpath `hadoop classpath`:. CastToMovieMappingDriver.java
jar cvf castToMovie.jar *.class
hadoop jar castToMovie.jar CastToMovieMappingDriver /user/av2783/theMovieProject/tmdb_5000_credits.tsv /user/av2783/theMovieProject/castToMovie