# Spark on docker
Link to canva presentation: 


https://www.canva.com/design/DAFY5iVJZm4/gxwor3lq2hWb1n7a9GuTCA/view?utm_content=DAFY5iVJZm4&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton

This is an example project setup with Spark and Hadoop. It contains:
* A Spark application in python which performs basic operations on data
* A system for data deployment on hdfs


In this example, the data contains informations about bike trip history data in Washington(https://s3.amazonaws.com/capitalbikeshare-data/index.html) - On this repository is just the part of the data, because .csv with all data is too big.
The basic idea of this example is to raise containers on docker, load the data into hdfs(namenode container) and do some
basic calculations that will give relevant informations about bike trips in Washington.

In the following, the way to launch complete applications and the meaning of certain parts of the code will be described.

## Requirements
* Docker(Docker Desktop)
* Docker Compose
* python(recommendation - PyCharm)
* pyspark


## How to create a network
First, we need network in order for the containers to be visible one to another, we will call it bde, open cmd and write:
```
docker network create bde
```
Tou can check your containers on Portainer.io
## How set up containers 
To build and start:
In the file explorer, navigate to the folder where the docker-compose file for spark and hadoop.env file is located, open cmd:
```
docker-compose up --build
```
Add the `-d` flag to hide the output. To view the output of a specific container, check the names of running containers with `docker ps` and then use `docker logs -f <container_name>`.

To shut down:

```
docker-compose down -v
```

To restart only one container after making any changes:

```
docker-compose up --build <container_name>
```
## Copy data to hdfs

Navigate to the folder where your data and .py files are, open cmd
copy .py to namenode:
```
docker cp projekat1NP.py namenode:/data 
```
copy Data 
```
docker cp Data namenode:/data 
```
Start hadoop:
```
docker exec -it namenode bash
```
you will se:
`prompt#`
Create folder in hdfs for your data(I will cal it proj):
```
hdfs dfs -mkdir /proj 
```
Next step is puting data on hdfs, in `prompt#`, same:
```
hdfs dfs -put /data/projekat1NP.py /proj
hdfs dfs -put /data/Data /proj
```

Or you can just open folder SparkPython and double click on `puttohdfs` that is a .bat script for putting data on hdfs.

## Manually starting the application
In folder where docker-compose and hadoop.env are, open cmd:
```
docker run -it --network bde --env-file hadoop.env -p 4040:4040 --name spark bde2020/spark-base:3.1.2-hadoop3.2 bash
```
Now you will see:
`bash-5.0#` here you starting your spark application
```
/spark/bin/spark-submit --master spark://fb1d85d74f9a:7077 --executor-memory 4g --total-executor-cores 4 hdfs://namenode:9000/dir/projekat1NP.py hdfs://namenode:9000/dir/Data "14th & Harvard St NW" "2016-01-01 00:00:00" "2016-02-02 00:00:00" 2600
```
`spark://483cc8cfe04d:7077` - address of your spark master, this is address from lolalhost:8070, you can write and 
real name of your spark master that is set into your docker-compose file, in this app is:  `spark://spark-master:7077`
`hdfs://namenode:9000/proj/projekat1NP.py hdfs://namenode:9000/proj/Data` - addresses of .py and Data for program in namenode.
`"14th & Harvard St NW" "2016-07-01 00:00:00" "2016-07-02 00:00:00" 2600` - application arguments from command line

## Starting the app using spark python template
Navigate into folder `templateSP` and open cmd. In DockerFile are informations for spark like application and data on hdfs namenode.
First step is to build docker image od application:
```
docker build --rm -t bde/spark-app .
```
Next is starting application:
```
docker run --name proj1np --net bde -p 4040:4040 -d bde/spark-app
```
Now you can look logs into your Docker Desktop app in proj1np. 

`Hopeful For A Positive Outcome.`