# POSTS_INPUT_PATH=/user/camille/finalproject/posts_input
# USERS_INPUT_PATH=/user/camille/finalproject/users_input
# INPUT_PARENT_PATH=/user/camille/finalproject/
# OUTPUT_PATH=/user/camille/finalproject/output

# # POSTS_FILE_PATH=posts_questions-000000000000.csv
# # USERS_FILE_PATH=users-000000000000.csv
# POSTS_FILE_PATH=test_posts.csv
# USERS_FILE_PATH=test_users.csv

# # Remove any old input
# hadoop fs -rm -r $INPUT_PARENT_PATH
# # Make input files
# hadoop fs -mkdir -p $POSTS_INPUT_PATH
# hadoop fs -mkdir -p $USERS_INPUT_PATH
# # Copy local input into hadoop
# hadoop fs -copyFromLocal $POSTS_FILE_PATH $POSTS_INPUT_PATH
# hadoop fs -copyFromLocal $USERS_FILE_PATH $USERS_INPUT_PATH

HADOOP_INPUT_PATH=/user/camille/generatemodel/input/
INPUT_FILE_PATH=/Users/camille/Northeastern/cs6240/6240-StackOverflow/linearregressiontest
# INPUT_FILE_PATH=/Users/camille/Northeastern/cs6240/6240-StackOverflow/sampledata_score,ansCount,avgScore.csv
# INPUT_FILE_PATH=/Users/camille/Northeastern/cs6240/6240-StackOverflow/smaller_sampledata_score,ansCount,avgScore.csv

hadoop fs -rm -r $HADOOP_INPUT_PATH
hadoop fs -mkdir -p $HADOOP_INPUT_PATH
hadoop fs -copyFromLocal $INPUT_FILE_PATH $HADOOP_INPUT_PATH