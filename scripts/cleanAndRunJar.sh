JAR_NAME=6240-StackOverflow.jar
GENERATE_MODEL_INPUT_PATH=/user/camille/generatemodel/input/
OUTPUT_PATH=/user/camille/generatemodel/output/

# hadoop dfsadmin –safemode leave
# hdfs dfsadmin -safemode leave

# Removes output file if it exists
hadoop fs -rm -r $OUTPUT_PATH
zip -d $JAR_NAME META-INF/LICENSE
# hadoop jar $JAR_NAME $GENERATE_MODEL_INPUT_PATH
hadoop jar $JAR_NAME $GENERATE_MODEL_INPUT_PATH $OUTPUT_PATH
