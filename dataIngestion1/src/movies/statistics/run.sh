#!/bin/bash
javac -classpath `hadoop classpath` MovieStatsMapperWithCounter.java
javac -classpath `hadoop classpath`:. MovieStatsDriver.java
jar cvf movieStats.jar *.class
hadoop jar movieStats.jar MovieStatsDriver /user/av2783/theMovieProject/movie_cleaned_data.tsv /user/av2783/theMovieProject/movieStatsAfterBadRowRemoval