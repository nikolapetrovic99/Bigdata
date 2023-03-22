# Standard libraries
import os
from datetime import datetime

# Pyspark libraries
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, dayofmonth, from_json, hour, max, mean, min, minute, month, round, second,
    stddev, year
)
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType
)
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer

# InfluxDB libraries
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

#from influx_writer import InfluxDBWriter

class InfluxDBWriter:
    def __init__(self):
        self._org = 'sparkbikesdb'
        self._token = 'f7bb5b113d8eede7e94b8574ba91e75e'
        self.client = InfluxDBClient(url="http://influxdb:8086", token=self._token, org=self._org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print(f"Opened {partition_id}, {epoch_id}")
        return True

    def process(self, row):
        self.write_api.write(bucket='sparkbikesdb', record=self._row_to_point(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print(f"Closed with error: {error}")

    def _row_to_point(self, row):
        print(row)
        start_datetime = datetime(
        int(row['Start year']),
        int(row['Start month']),
        int(row['Start day']),
        int(row['Start hour']),
        int(row['Start minute']),
        int(row['Start second'])
        )
        point = (
            Point.measurement("durationPrediction")
            .tag("measure", "durationPrediction")
            .time(datetime.utcnow(), WritePrecision.NS)
            .field("Start datetime", start_datetime.isoformat())  # Convert the datetime object to an ISO 8601 formatted string
            .field("Start station", str(row['start_station_index']))
            .field("End station", str(row['end_station_index']))
            .field("Member type", str(row['member_type_index']))
            .field("Bike number", str(row['Bike_number_index']))
            .field("Duration", int(row['Duration']))
            .field("prediction", int(row['prediction']))
        )
        return point


if __name__ == '__main__':

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
    ])

    spark = SparkSession.builder.appName("Rides").getOrCreate()
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
    dataset = dataset.where((col('Duration') >= 500) & (col('Duration') <= 2500))

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


    indexer_model = PipelineModel.load(os.environ["INDEXER_MODEL"])
    dataset = indexer_model.transform(dataset)

    dataset = dataset.drop("Start station")
    dataset = dataset.drop("End station")
    dataset = dataset.drop("Member type")
    dataset = dataset.drop("Bike number")

    dataset = dataset.select(*([col(c) for c in dataset.columns if c != 'Duration'] + [col('Duration')]))
    dataset.printSchema()

    dataset_copy = dataset.alias("dataset_copy")
    
    # Select all columns except the target (Duration)
    feature_cols = dataset_copy.columns[:-1]

    # Create a VectorAssembler instance to combine the feature columns
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Transform the DataFrame to include the combined feature column
    dataset_copy = assembler.transform(dataset_copy)
    dataset_copy.printSchema()

    """scaler_model = PipelineModel.load(os.environ["SCALER_MODEL"])
    # Scale the training data
    dataset_copy = scaler_model.transform(dataset_copy)
    dataset_copy.printSchema()"""
    
    model = PipelineModel.load(os.environ["REGRESSION_MODEL"])
    prediction = model.transform(dataset_copy)
    prediction.printSchema()
    """print(f"> Stampanje u konzoli ...")
    query = (prediction
            #.withWatermark("timestamp", "1 minute")
            .writeStream
            .outputMode("update")
            .queryName("DeesriptiveAnalysis")
            .format("console")
            .trigger(processingTime="5 seconds")
            .option("truncate", "false")
            #.foreachBatch(writeToCassandra)
            .start()
    )"""
    print(f"> Reading the stream and storing ...")
    query = (prediction
        .writeStream
        .foreach(InfluxDBWriter())
        .start()
        )

    query.awaitTermination()
    #spark.streams.awaitAnyTermination()


    #spark.stop()

