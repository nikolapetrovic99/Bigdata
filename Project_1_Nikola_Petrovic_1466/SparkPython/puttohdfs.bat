@echo off
docker cp projekat1NP.py namenode:/data 
docker cp Data namenode:/data 
docker exec -it namenode bash -c "hdfs dfs -mkdir /dir"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/projekat1NP.py"
docker exec -it namenode bash -c "hdfs dfs -rm -r /dir/Data"
docker exec -it namenode bash -c "hdfs dfs -put /data/projekat1NP.py /dir"
docker exec -it namenode bash -c "hdfs dfs -put /data/Data /dir"
