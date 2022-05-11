#!/bin/bash
javac -classpath `hadoop classpath` GenreMapper.java
javac -classpath `hadoop classpath` GenreReducer.java
javac -classpath `hadoop classpath`:. GenreMappingDriver.java
jar cvf genre.jar *.class
hadoop jar genre.jar GenreMappingDriver /user/av2783/theMovieProject/tmdb_5000_movies.tsv /user/av2783/theMovieProject/genre