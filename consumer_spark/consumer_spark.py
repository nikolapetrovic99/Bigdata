import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, window, desc
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import window, col, avg, min, max, count



def writeToCassandra(df, epochId):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sparkone", keyspace="newkeyspace") \
        .mode("append") \
        .save()


conf = SparkConf()
#conf.setMaster("spark://spark-master:7077")   #("spark://spark-master:7077")
conf.setMaster("local")
conf.set("spark.driver.memory","4g")

#cassandra
conf.set("spark.cassandra.connection.host", "cassandra")
conf.set("spark.cassandra.connection.port", "9042")
#conf.set("spark.cassandra.auth.username", "cassandra")
#conf.set("spark.cassandra.auth.password", "cassandra")


spark = SparkSession.builder.config(conf=conf).appName("Bike rides").getOrCreate()


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

#df=spark.read.option("inferSchema", True).option("header", True).csv("C:/Users/Nikola Petrovic/Desktop/bigdata/kafka/kafka-spark-flink-container-main/data/2016Q1-capitalbikeshare-tripdata.csv")
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
parsed_values = df.select(
    "timestamp", from_json(col("value").cast("string"), schema).alias("parsed_values")
)
print("Podaci su :\n")
print(parsed_values)

print("\n\n")
print()

# Convert columns to actual data types


print("\n\n")
# Adding column id, we need primary key, we are working with a real database
#parsed_values = parsed_values.withColumn("id", monotonically_increasing_id())

print("Podaci su nakon parsiranja i pretvaranja u kolone:\n")
print("\n\n")


durations = parsed_values.selectExpr("timestamp", "parsed_values.Duration AS Duration", "parsed_values[\"Start station\"] as Start_station")
windowDuration = "10 seconds"  # The length of the window
slideDuration = "5 seconds"  # The sliding interval
"""averageDuration = durations.groupBy(durations.Start_station, window(durations.timestamp, windowDuration, slideDuration)).agg(
    avg("Duration").alias("avg_duration"),
    min("Duration").alias("min_duration"),
    max("Duration").alias("max_duration"),
    count("Start_station").alias("start_station_count")
)#.dropDuplicates()
averageDuration.dropDuplicates()
query = (averageDuration
         #.withWatermark("timestamp", "1 minute")
         .writeStream
         .outputMode("update")
         .queryName("DeesriptiveAnalysis")
         .format("console")
         .trigger(processingTime="5 seconds")
         .option("truncate", "false")
         #.foreachBatch(writeToCassandra)
         .start()
         .awaitTermination())"""

popular_start_stations = (
    durations
    .groupBy(durations.Start_station, window(durations.timestamp, windowDuration, slideDuration))
    .count()
    .orderBy(desc("count"))
)
N = 5
top_N_start_stations = popular_start_stations.limit(N)

query = (
    top_N_start_stations
    .writeStream
    .outputMode("complete")
    .queryName("top_N_start_stations")
    .format("console")
    .start()
    .awaitTermination()
)
"""query = (
    parsed_values.writeStream.outputMode("update")
    .queryName("rides")
    .format("console")
    .trigger(processingTime="5 seconds")
    .option("truncate", "false")
    .foreachBatch(writeToCassandra)
    .start()
    .awaitTermination()
)"""
#durations = parsed_values.selectExpr("timestamp", "parsed_values.Duration AS Duration")
# We set a window size of 10 seconds, sliding every 5 seconds.
#averageDuration= durations.groupBy(window(durations.timestamp, "4 seconds", "2 seconds")).agg({"Duration": "average"})
#averageDuration.printSchema()
