#!/bin/bash
javac -classpath `hadoop classpath` MovieRemoveBadRowsMapper.java
javac -classpath `hadoop classpath`:. MovieRemoveBadRowsDriver.java
jar cvf movieRemoveBadRows.jar *.class
hadoop jar movieRemoveBadRows.jar MovieRemoveBadRowsDriver /user/av2783/theMovieProject/tmdb_movies_filtered_columns.tsv /user/av2783/theMovieProject/movieCleanedData