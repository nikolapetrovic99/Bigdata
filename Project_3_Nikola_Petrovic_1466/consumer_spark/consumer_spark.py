import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import (
    col, lit, isnan, isnull, count, when, min, max, avg, mean, variance, stddev, skewness, kurtosis,
    hour, trunc, round, date_format, monotonically_increasing_id, to_timestamp, desc, asc,
    year, month, dayofmonth, minute, second, from_json, window
)
from pyspark import SparkConf
from pyspark.ml.feature import StringIndexer, VectorAssembler, StringIndexerModel
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
#from influx_writer import InfluxDBWriter

class InfluxDBWriter:
    def __init__(self, approaches, cloud=False):
        self.url = "http://influxdb:8086"
        self.token = "f7bb5b113d8eede7e94b8574ba91e75e"
        self.org = "anjebza"
        self.bucket = "telegraf"
        #self.approaches = approaches
        if cloud: # Connect to InfluxDB Cloud
            self.client = InfluxDBClient(
                url="<cloud.url>", 
                token="<cloud.token>", 
                org="<cloud.org>"
            )
        else: # Connect to a local instance of InfluxDB
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        # Create a writer API
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True
    
    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))
    
    def _row_to_point(self, row):
        print(row)
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        return Point.measurement("tabela").tag("measure", "tabela") \
                    .time(timestamp, write_precision='ms') \
                    .field("Start date", int(row['Start date'])) \
                    .field("Start station number", int(row['Start station number'])) \
                    .field("Start station", String(row['Start station'])) \
                    .field("End station number", int(row['End station number'])) \
                    .field("End station", String(row['End station'])) \
                    .field("Bike number", String(row['bike number'])) \
                    .field("Member type", String(row['Member type'])) \
                    .field("Duration", int(row['Duration'])) \
                    .field("prediction", int(row['prediction']))
    def process(self, row):
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=self._row_to_point(row))
        except Exception as ex:
            print(f"[x] Error {str(ex)}")

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--N", type=int, help="The number of top start stations to select")
    args = parser.parse_args()

    N = args.N or 5  # Default to 5 if N is not provided

    schema = StructType([
        StructField("Duration", IntegerType(), False),
        StructField("Start date", TimestampType(), False),
        StructField("End date", TimestampType(), False),
        StructField("Start station number", IntegerType(), False),
        StructField("Start station", StringType(), False),
        StructField("End station number", IntegerType(), False),
        StructField("End station", StringType(), False),
        StructField("Bike number", StringType(), False),
        StructField("Member type", StringType(), False),
        #StructField("timestamp", TimestampType(), False)
    ])

    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    #conf.setMaster("local")
    conf.set("spark.driver.memory","4g")

    #cassandraC
    #conf.set("spark.cassandra.connection.host", "cassandra")
    #conf.set("spark.cassandra.connection.port", "9042")
    #conf.set("spark.cassandra.auth.username", "cassandra")
    #conf.set("spark.cassandra.auth.password", "cassandra")

    spark = SparkSession.builder.config(conf=conf).appName("Rides").getOrCreate()

    # Get rid of INFO and WARN logs.
    spark.sparkContext.setLogLevel("ERROR")

    

    dataset = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_HOST"])
        #.option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", os.environ["KAFKA_TOPIC"])
        #.option("subscribe", "bikes-spark")
        .option("startingOffsets", "latest")
        .option("groupIdPrefix", os.environ["KAFKA_CONSUMER_GROUP"])
        #.option("groupIdPrefix", "Spark-Group")
        .load()
        .selectExpr("CAST(value AS STRING)")
    )
    dataset = dataset.select(from_json(col("value"), schema).alias("dataset")).select("dataset.*")
    dataset.printSchema()
    

    # Remove end date from dataset for regression model and calculating duration
    dataset = dataset.drop("End date") \
                .withColumn("Start year", year("Start date")) \
                .withColumn("Start month", month("Start date")) \
                .withColumn("Start day", dayofmonth("Start date")) \
                .withColumn("Start hour", hour("Start date")) \
                .withColumn("Start minute", minute("Start date")) \
                .withColumn("Start second", second("Start date")) \
                .drop("Start date")

    dataset = dataset.drop("Start station number")
    dataset = dataset.drop("End station number")


    indexer_model = PipelineModel.load("hdfs://namenode:9000/dir/modelIndexer")
    dataset = indexer_model.transform(dataset)

    dataset = dataset.drop("Start station")
    dataset = dataset.drop("End station")
    dataset = dataset.drop("Member type")
    dataset = dataset.drop("Bike number")


    dataset = dataset.select(*([col(c) for c in dataset.columns if c != 'Duration'] + [col('Duration')]))
    dataset.printSchema()

    """query = (dataset
            #.withWatermark("timestamp", "1 minute")
            .writeStream
            .outputMode("update")
            .queryName("DesriptiveAnalysis")
            .format("console")
            .trigger(processingTime="5 seconds")
            .option("truncate", "false")
            #.foreachBatch(writeToCassandra)
            .start()
    )
    query.awaitTermination()"""

    dataset_copy = dataset.alias("dataset_copy")
    model = PipelineModel.load(os.environ["REGRESSION_MODEL"])

    # Select all columns except the target (Duration)
    feature_cols = dataset_copy.columns[:-1]

    # Create a VectorAssembler instance to combine the feature columns
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Transform the DataFrame to include the combined feature column
    dataset_copy = assembler.transform(dataset_copy)

    prediction = model.transform(dataset_copy)


    
    print(f"> Reading the stream and storing ...")
    query = (prediction
        .writeStream
        .foreach(InfluxDBWriter( """approaches = sys.argv[1:]""" ))
        #.option("checkpointLocation", "checkpoints")
        .start())

    query.awaitTermination()
    #spark.streams.awaitAnyTermination()
    

    spark.stop()

