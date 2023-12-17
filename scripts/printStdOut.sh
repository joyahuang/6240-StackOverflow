USER_LOGS_PATH=~/Northeastern/cs6240/hadoop/hadoop-3.3.1/logs/userlogs

cd $USER_LOGS_PATH
latest_run_application_id=$(ls | sort -V | tail -n 1)
echo "Printing output for application ID: '$latest_run_application_id'"
cat $latest_run_application_id/container*/stdout