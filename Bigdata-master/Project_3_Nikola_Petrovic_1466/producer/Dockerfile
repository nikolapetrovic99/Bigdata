FROM python:3.8-slim
WORKDIR /
RUN pip install kafka-python==2.0.2
RUN pip install numpy
COPY . .
CMD python -u $SCRIPT