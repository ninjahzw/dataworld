#/bin/bash

project_path=/Users/houzhaowei/IdeaProjects/hidata
remote_host=192.168.1.140
remote_user=root
hadoop_input=/user/hive/warehouse/main_info/dt=20131101
hadoop_output=/user/hive/warehouse/outaduid
jar_file=/data/workspace/map-reduce-jar-with-dependencies.jar

echo '-- mvn package --'
cd $project_path && mvn -U package 

echo '-- copy jar file to remote --'
scp $project_path/target/map-reduce-jar-with-dependencies.jar $remote_user@$remote_host:/data/workspace 

echo '-- run remote MR task --'
ssh $remote_user@$remote_host '/usr/bin/hadoop jar '$jar_file $hadoop_input $hadoop_output '-Dmapred.reduce.tasks=9'

echo '-- result --'
#ssh $remote_user@$remote_host '/usr/bin/hadoop  fs -cat '$hadoop_output'/part-r-00000 | head'
