FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy
RUN cd /app pip install -r requirements.txt

ENV REGRESSION_MODEL='hdfs://namenode:9000/dir/modelData'
ENV KAFKA_HOST=kafka:9092
ENV KAFKA_TOPIC=bikes-spark
ENV KAFKA_CONSUMER_GROUP=Spark-Group
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/consumer_spark.py
ENV SPARK_APPLICATION_ARGS "--N 10"
ENV SPARK_SUBMIT_ARGS --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 --executor-memory 1G --executor-cores 1
