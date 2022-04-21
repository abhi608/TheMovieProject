#!/bin/bash
javac -classpath `hadoop classpath` CreditStatsMapperWithCounter.java
javac -classpath `hadoop classpath`:. CreditStatsDriver.java
jar cvf creditAnalysis.jar *.class
hadoop jar creditAnalysis.jar CreditStatsDriver /user/av2783/theMovieProject/tmdb_5000_credits.tsv /user/av2783/theMovieProject/creditStats