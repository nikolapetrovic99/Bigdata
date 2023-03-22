# pyspark-kafka-influxdb-grafana

Link to canva presentation:


https://www.canva.com/design/DAFbb8kpLQw/7XWqn8LDcNnsMx8kSciZbQ/view?utm_content=DAFbb8kpLQw&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton


This is an example project setup with Kafka, Spark and Regression model. It contains:
* A Kafka producer sending data over a topic/s, 
* A consumer processing the data stream using Spark with Structured Streaming in py,
* A docker-compose file to start up and shut down all kafka and spark containers.
* A consumer_spark folder with whole configuration and a .py program for kafka consumer in spark.
* A requirements.txt with pip installs.
* A regression folder for creating models for regression, indexing and scaling
* Influx-grafana folder for vizualisation of data

In this example, the data is sent in JSON format containing informations about bike trip history data in Washington(https://s3.amazonaws.com/capitalbikeshare-data/index.html) - On this repository is just the part of the data, because .csv with all data is too big..
Both spark and flink consumers first read data from kafka producer, and deserialize it in acceptable format. Then, using a technique of sliding window
takes some default informations of data, like mean, average, min, max. Next is top N most popular locations.
Applications are simple, and main goal is a representation of how spark and flink can communicate with kafka producer.

In the following, the way to launch complete applications and the meaning of certain parts of the code will be described.

## Requirements
* Docker(Docker Desktop)
* Docker Compose
* python(recommendation - vscode)
* Influxdb
* influxdb_client
* numpy
How to run and build application 
To build and start:
In the file explorer, navigate to the folder where the docker-compose file for spark and kafka applications is located, open cmd:
```
docker-compose up --build
```
Do this step also in folders: `influxdb-grafana` and `spark-hadoop`
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
* namenode
* resourcemanager
* datanode
* nodemanager
* historyserver

All of the containers are on the same network: bde.

## Storing data on hdfs
Next step is to store data for regression on hdfs:

Navigate to folder where your `train` and `test` `.csv` data is and open cmd.
Commands(This is just example):
```
docker cp Data namenode:/Data
```
```
docker exec -it namenode bash
```
Creationg of random folder `dir` in hdfs:
```
hdfs dfs -mkdir /dir
```
Storing folder Data(which contains train and test folders/.csv-s) from namenode to folder dir in hdfs:
```
hdfs dfs -put /Data/Data /dir
```
## Regression - modelIndexer, modelData
Next step is creating image for regression, in regression folder, open cmd and write:
```
docker build --rm -t bde/spark-app-regression .
```
This will create image.

Running the program:
```
docker run --name proj3-regression --net bde -p 4040:4040 -d bde/spark-app-regression
```
In regression.py are methods for storing modelData and modelIndexer on hdfs, which will be used for indexing and prediction of data, respectively.

## Spark applcation
Next step is creating image of consumer spark, in consumer_spark folder, open cmd and write:
```
docker build --rm -t bde/spark-app-consumer .
```
This will build image.

Running the program:
```
docker run --name proj3-consumer --net bde -p 4041:4041 -d bde/spark-app-consumer
```

## Influxdb 
After running the consumer, data will be writen into influxdb.
This program needs `influxdb` and `influxdb_client` for writing in database.
There we will use manual installation of this imports because of old version of python in out spark image.

Steps are:

Open the terminal of spark-master container, and all the workers you have
(you can open it directly from docker desktop or with a command in cmd: `docker exec -it spark-worker-2 bash`),
and write next commands, in all of them, respectively:
```
pip install influxdb
```

```
pip3 install influxdb-client
```

```
apk add --no-cache py3-numpy
```

This is hardcoded way, but it works.

You can theck your influxdb data directly in docker desktop, open terminal of influxdb in docker desktop and write next commands:
You are reading from database with name:`sparkbikesdb` and from table(measurement): `tabela`
```
influx
```
```
use sparkbikesdb
```
```
select * from tabela
```


## Grafana
When you have your data in influxdb, you can look at grafana:
Steps in grafana for data vizualisation:

Open grafana:
`https://localhost:3000` or from docker desktop directly.

Go to `Add your first datasource` and select InfluxDB

This will open window for configuration
Name: `InfluxDB` it is default and you dont need to worry about it
Query language: `InfluxQL` also default
URL: `http:/influxdb:8086`

In defaults `Basic auth` is ON, set it OFF

Database: `sparkbikesdb`
User:`admin`
Password:`admin`

Save & test

Next step is click on menu on `+ new Dashboard`
Go to `Add a new panel`

You will see default configuration.
Select your datasource, in our case `InfluxDB'
Select your measurement
Remove `group by`
Remove `mean()`
Go on `+` and add your predictions or whatever, and thats it, you will se your predictions.
Next step is `Save`, give your dashboard a name.

