from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

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
        self.write_api = self.client.write_api()

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
                    .field("Start date", int(row['Start date'])) \
                    .field("Start station number", int(row['Start station number'])) \
                    .field("Start station", String(row['Start station'])) \
                    .field("End station number", int(row['End station number'])) \
                    .field("End station", String(row['End station'])) \
                    .field("Bike number", String(row['bike number'])) \
                    .field("Member type", String(row['Member type'])) \
                    .field("Duration", int(row['Duration'])) \
                    .field("prediction", int(row['prediction'])) \
                    .time(timestamp, write_precision='ms')
    def process(self, row):
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=self._row_to_point(row))
        except Exception as ex:
            print(f"[x] Error {str(ex)}")