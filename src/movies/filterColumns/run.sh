#!/bin/bash
javac -classpath `hadoop classpath` MovieFilterColumnMapper.java
javac -classpath `hadoop classpath`:. MovieFilterColumnDriver.java
jar cvf movieFilterColumns.jar *.class
hadoop jar movieFilterColumns.jar MovieFilterColumnDriver /user/av2783/theMovieProject/tmdb_5000_movies.tsv /user/av2783/theMovieProject/movieFiltered