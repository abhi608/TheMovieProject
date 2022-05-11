SHELL COMMANDS :

For all tasks these 4 four commands were used :

javac -classpath `hadoop classpath` MapperName.java
javac -classpath `hadoop classpath` ReducerName.java
javac -classpath `hadoop classpath`:. DriverName.java
jar cvf jarFileName.jar *.class
hadoop jar jarFileName.jar DriverClassName <InputPathInHDFS> <OutputPathInHDFS> 
