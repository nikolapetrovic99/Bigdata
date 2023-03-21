from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

class InfluxDBWriter:
    def __init__(self, cloud=False):
        self.url = "http://influxdb:8086"
        self.token = "f7bb5b113d8eede7e94b8574ba91e75e"
        self.org = "anjebza"
        self.bucket = "telegraf"
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
        # String to timestamp
        timestamp = datetime.strptime(row["timestamp"], "%d/%m/%Y %H:%M:%S.%f %p")
        print(f"> Processing {timestamp}")
        return Point.measurement("tabela").tag("measure", "tabela") \
                    .time(timestamp) \
                    .field("Start date", int(row['Start date'])) \
                    .field("Start station number", int(row['Start station number'])) \
                    .field("Start station", str(row['Start station'])) \
                    .field("End station number", int(row['End station number'])) \
                    .field("End station", str(row['End station'])) \
                    .field("Bike number", str(row['bike number'])) \
                    .field("Member type", str(row['Member type'])) \
                    .field("Duration", int(row['Duration'])) \
                    .field("prediction", int(row['prediction']))
    def process(self, row):
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=self._row_to_point(row))
        except Exception as ex:
            print(f"[x] Error {str(ex)}")