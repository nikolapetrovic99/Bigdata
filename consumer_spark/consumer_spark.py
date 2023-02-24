import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, window, desc
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import window, col, avg, min, max, count, mean



def writeToCassandra(df, epochId):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sparkone", keyspace="newkeyspace") \
        .mode("append") \
        .save()
    df.show()
def writeToCassandra1(df, epochId):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sparktwo", keyspace="newkeyspace") \
        .mode("append") \
        .save()
    df.show()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--N", type=int, help="The number of top start stations to select")
    args = parser.parse_args()

    N = args.N or 5  # Default to 5 if N is not provided
    
    conf = SparkConf()
    #conf.setMaster("spark://spark-master:7077")
    conf.setMaster("local")
    conf.set("spark.driver.memory","4g")

    #cassandra
    conf.set("spark.cassandra.connection.host", "cassandra")
    conf.set("spark.cassandra.connection.port", "9042")
    #conf.set("spark.cassandra.auth.username", "cassandra")
    #conf.set("spark.cassandra.auth.password", "cassandra")

    spark = SparkSession.builder.config(conf=conf).appName("Rides").getOrCreate()

    # Get rid of INFO and WARN logs.
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_HOST"])
        #.option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", os.environ["KAFKA_TOPIC"])
        #.option("subscribe", "bikes-spark")
        .option("startingOffsets", "latest")
        .option("groupIdPrefix", os.environ["KAFKA_CONSUMER_GROUP"])
        #.option("groupIdPrefix", "Spark-Group")
        .load()
    )

    schema = StructType([
        StructField("Duration", StringType(), False),
        StructField("Start date", TimestampType(), False),
        StructField("End date", TimestampType(), False),
        StructField("Start station number", StringType(), False),
        StructField("Start station", StringType(), False),
        StructField("End station number", StringType(), False),
        StructField("End station", StringType(), False),
        StructField("Bike number", StringType(), False),
        StructField("Member type", StringType(), False),
        StructField("timestamp", TimestampType(), False)
    ])


    # Parse the "value" field as JSON format and cast the columns to the appropriate data types
    parsed_values = df.select("timestamp", from_json(col("value").cast("string"), schema).alias("parsed_values"))


    durations = parsed_values.selectExpr("timestamp", "parsed_values.Duration AS Duration", "parsed_values[\"Start station\"] as start_station")
    windowDuration = "10 seconds"  # The length of the window
    slideDuration = "10 seconds"  # The sliding interval

    durationInfo = durations.groupBy(durations.start_station, window(durations.timestamp, windowDuration, slideDuration)).agg(
        avg("Duration").alias("avg_duration"),
        min("Duration").alias("min_duration"),
        mean("Duration").alias("mean_duration"),
        max("Duration").alias("max_duration"),
        count("Start_station").alias("start_station_count"),
        col("window.start").alias("start_date"),
        col("window.end").alias("end_date")
    ).drop("window")#.dropDuplicates()

    durationInfo.printSchema()

    query = (durationInfo
            #.withWatermark("timestamp", "1 minute")
            .writeStream
            .outputMode("update")
            .queryName("DeesriptiveAnalysis")
            #.format("console")
            #.trigger(processingTime="5 seconds")
            #.option("truncate", "false")
            .foreachBatch(writeToCassandra)
            .start()
    )


    popular_start_stations = (durations
        .groupBy(durations.start_station, window(durations.timestamp, windowDuration, windowDuration))
        .agg(count("*").alias("popularity_count"))
        .orderBy(desc("popularity_count"))
        .select(col("start_station"), col("popularity_count"), col("window.start").alias("start_date"), col("window.end").alias("end_date"))
    )


    top_N_start_stations = popular_start_stations.limit(N)
    top_N_start_stations.printSchema();


    query1 = (
        top_N_start_stations
        .writeStream
        .outputMode("complete")
        .queryName("top_N_start_stations")
        #.format("console")
        .trigger(processingTime="5 seconds")
        #.option("truncate", "true")
        .foreachBatch(writeToCassandra1)
        .start()
    )

    query.awaitTermination()
    query1.awaitTermination()

