#/bin/bash

project_path=/Users/houzhaowei/IdeaProjects/hidata
remote_host=114.245.34.180
remote_user=root
hadoop_input=/user/hive/warehouse/main_info/dt=20131101/
hadoop_output=/user/hive/warehouse/output_5/1
hadoop_output2=/user/hive/warehouse/output_5/1
jar_file=/data/workspace/map-reduce-jar-with-dependencies.jar

echo '-- mvn package --'
cd $project_path && mvn -U package 

echo '-- copy jar file to remote --'
scp $project_path/target/map-reduce-jar-with-dependencies.jar $remote_user@$remote_host:/data/workspace 

echo '-- run remote MR task --'
#ssh $remote_user@$remote_host '/usr/bin/hadoop jar '$jar_file $hadoop_input $hadoop_output $hadoop_output2 '-Dmapred.reduce.tasks=4'

echo '-- result --'
#ssh $remote_user@$remote_host '/usr/bin/hadoop  fs -cat '$hadoop_output'/part-r-00000 | head'
