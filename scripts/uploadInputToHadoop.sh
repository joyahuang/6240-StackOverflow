HADOOP_INPUT_PATH=/user/camille/generatemodel/input/
# INPUT_FILE_PATH=/Users/camille/Northeastern/cs6240/6240-StackOverflow/linearregressiontest
# INPUT_FILE_PATH=/Users/camille/Northeastern/cs6240/6240-StackOverflow/innerjoin.csv
INPUT_FILE_PATH=/Users/camille/Northeastern/cs6240/6240-StackOverflow/innerjoinfulldata.csv

hadoop fs -rm -r $HADOOP_INPUT_PATH
hadoop fs -mkdir -p $HADOOP_INPUT_PATH
hadoop fs -copyFromLocal $INPUT_FILE_PATH $HADOOP_INPUT_PATH
