FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy
# Set environment variables
ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/regression.py
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/dir/Data"
ENV SPARK_SUBMIT_ARGS --executor-memory 3G --executor-cores 3
