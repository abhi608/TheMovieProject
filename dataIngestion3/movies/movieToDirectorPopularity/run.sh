#!/bin/bash
javac -classpath `hadoop classpath` MovieToCastMapper.java
javac -classpath `hadoop classpath` MovieToCastReducer.java
javac -classpath `hadoop classpath`:. MovieToCastMappingDriver.java
jar cvf movieToDirToPop.jar *.class
hadoop jar movieToDirToPop.jar MovieToCastMappingDriver /user/ss14396/rproject/finalMoviesAvg.csv /user/ss14396/directorPopularityOutput/