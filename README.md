# Kafka Spark Flink Docker python java Docker Desktop Windows

This is an example project setup with Kafka, Spark and Flink. It contains:
* A Kafka producer sending data over a topic/s,
* A consumer processing the data stream using Spark with Structured Streaming in py,
* A consumer processing the data stream using Flink in java,
* A docker-compose file to start up and shut down all kafka and spark containers.
* A cassandra-conf foldder with docker-compose for cassandra container.
* A consumer_spark folder with whole configuration and a .py program for kafka consumer in spark.
* A flink-java foder with java aapplication for flink-kafka consumer.
* A requirements.txt with pip installs(in this case empty).

In this example, the data is sent in JSON format containing informations about bike trip history data in Washington(https://s3.amazonaws.com/capitalbikeshare-data/index.html).
Both spark and flink consumers first read data from kafka producer, and deserialize it in acceptable format. Then, using a technique of sliding window
takes some default informations of data, like mean, average, min, max. Next is top N most popular locations.
Applications are simple, and main goal is a representation of how spark and flink can communicate with kafka producer.

In the following, the way to launch complete applications and the meaning of certain parts of the code will be described.

## Requirements
* Docker(Docker Desktop)
* Docker Compose
* python(recommendation - vscode)
* java 1.8(recommendation - IntelliJ)
* 
## How to run and build application 
To build and start:
In the file explorer, navigate to the folder where the docker-compose file for spark and kafka applications is located, open cmd:
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
For example, `docker-compose up --build consumer_spark`.

Now, when your docker is up, you need cassandra:

In the file explorer, navigate to the folder where the docker-compose file for cassandra is, open cmd:
```
docker-compose up -d
```
## kafka-spark
Containers you have now:
* zookeper
* kafka
* producer
* spark-master and two stark-workers

All of the containers are on the same network: bde.

Next step is creating image, in consumer_spark folder, open cmd and write:
```
docker build --rm -t bde/spark-app .
```
This will build image.

If we want to write to cassandra, we need keyspace and tables in it.
First step is to open a cassandra shell. You can do that manualy, or easier way:
open Docker Desktop, and click on the cassandra container, you will see terminal.
To connect to cassandra write:
```
cqlsh
```
You may need to wait a while.
After that, create keyspace:
```
CREATE KEYSPACE newkeyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```
Next step is creating a tables, in our spark application, name of the tables are: `sparkone` and `sparktwo`.
Creating looks like this, in this specific case:
```
CREATE TABLE IF NOT EXISTS newkeyspace.sparkone (start_station text, avg_duration double, max_duration text, mean_duration double, min_duration text, start_station_count bigint, start_date timestamp, end_date timestamp, PRIMARY KEY (start_station, start_date, end_date))

CREATE TABLE IF NOT EXISTS newkeyspace.sparktwo(start_station text, popularity_count bigint, start_date timestamp, end_date timestamp, PRIMARY KEY (start_station, start_date, end_date));
```
Note that cassandra is case-INsensitive, but sometimes can be confused, so write your code like it is case-sensitive.

You can delete table with:
```
drop table newkeyspace.sparkone;
```
Or clear it with:
```
truncate table newkeyspace.sparkone;
```
Select all:
```
select * from newkeyspace.sparkone;
```

## RUN
In consumer_spark folder, open cmd and write:
```
docker run --name proj2 --net bde -p 4040:4040 -d bde/spark-app
```
Args of spark application are `N`, and it is set in DockerFile in `consumer_spark`.

Now wait for errors :P

## kafka-flink
In flink-java folder, open cmd and write:
```
docker-compose up -d
```
Now you have containers:
* jobmanager
* taskmanager

From jobmanager, you can open web UI by clicking on port in Docker Desktop.
You need to build jar fale from your java application, you can do that on this way:

In flink-java folder, open cmd and write:
```
mvn clean package
```
On web UI you will select option `Submit new job --> + Add new`, and select your created jar file which is in target folder,
in this case is called: `flink-java-1.0-SNAPSHOT`
By clicking at new added jar in web UI, there need to be: `projekat.DataStreamJob` in first textbox,
in text box "Program Arguments" you can write: `5 "Lincoln Memorial" "15th & P St NW"` - or stations which you like, the number of stations is not limited



## How to use
In the docker-compose, you can specify the following variables:
* Name of the data source that should be sent over the Kafka topic. You can replace this by any CSV file with headers.
* Name of the Kafka topic.
* The time interval that the Kafka producer uses to send data.

You can write your Spark or Flink jobs in the respective scripts and run the corresponding Docker container to start the job.

The Spark UI is accessible through http://localhost:4040.

### Adding new data
To add new data, add a CSV file with headers to the data/ directory, put the filename in the 'DATA' environment variable in docker-compose.yml, and rebuild the container `producer`. You can then adjust the schemas and jobs of the consumer(s).
