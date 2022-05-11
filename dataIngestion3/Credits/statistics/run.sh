#!/bin/bash
javac -classpath `hadoop classpath` CreditStatsMapperWithCounter.java
javac -classpath `hadoop classpath`:. CreditStatsDriver.java
jar cvf creditAnalysis.jar *.class
hadoop jar creditAnalysis.jar CreditStatsDriver /user/ss14396/rproject/credits.tsv /user/ss14396/rproject/creditStats
