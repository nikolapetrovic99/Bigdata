FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/projekat1NP.py
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/proj/Data" "14th & Harvard St NW" "2016-07-01 00:00:00" "2016-07-02 00:00:00" 2600
ENV SPARK_SUBMIT_ARGS --executor-memory 3G --executor-cores 3