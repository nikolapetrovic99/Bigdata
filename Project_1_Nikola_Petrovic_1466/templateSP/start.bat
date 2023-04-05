@echo off
docker build --rm -t bde/spark-app .
docker run --name proj1np --net bde -p 4040:4040 -d bde/spark-app
