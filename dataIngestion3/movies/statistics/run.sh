#!/bin/bash
javac -classpath `hadoop classpath` MovieStatsMapperWithCounter.java
javac -classpath `hadoop classpath`:. MovieStatsDriver.java
jar cvf movieStats.jar *.class
hadoop jar movieStats.jar MovieStatsDriver /user/ss14396/rproject/movies_metadata.tsv /user/ss14396/rproject/movieStats