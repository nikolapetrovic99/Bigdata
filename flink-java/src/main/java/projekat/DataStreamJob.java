/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package projekat;

import models.BikesTrip;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import javax.naming.Context;


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
    public class DataStreamJob {

    public static DataStream<BikesTrip> ConvertStreamFromJsonToBikesTripType(DataStream<String> jsonStream) {
        return jsonStream.map(kafkaMessage -> {
            try {
                JsonNode jsonNode = new ObjectMapper().readValue(kafkaMessage, JsonNode.class);
                return new BikesTrip(jsonNode.get("Duration").asInt(),
                        jsonNode.get("Start date").asText(),
                        jsonNode.get("End date").asText(),
                        jsonNode.get("Start station number").asInt(),
                        jsonNode.get("Start station").asText(),
                        jsonNode.get("End station number").asInt(),
                        jsonNode.get("End station").asText(),
                        jsonNode.get("Bike number").asText(),
                        jsonNode.get("Member type").asText());

            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull).forward();
    }
        public static void main(String[] args) throws Exception {

            int n;
            List<String> locations;
            if (args.length < 1) {
                System.out.println("Please provide at least one argument for n");
                return;
            }
            else{
                // Parse program arguments
                n = Integer.parseInt(args[0]);
                locations = new ArrayList<>(Arrays.asList(args).subList(1, args.length));
            }

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            String inputTopic = "bikes-spark";
            //String server = "localhost:9091";/*app na lokalu*/
            String server = "kafka:9092";/*app na kontejneru*/

            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(server)
                    .setTopics(inputTopic)
                    .setGroupId("my-group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();
            //DataStream<String> dataStream = StreamConsumer(inputTopic, server, env);
            //dataStream.print();
            DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
            //text.print();
            DataStream<BikesTrip> tripDataStream = ConvertStreamFromJsonToBikesTripType(text);
            //tripDataStream.print();
            //List<String> locations = Arrays.asList("Lincoln Memorial", "15th & P St NW");

            DataStream<BikesTrip> newTripData1 = tripDataStream.rebalance();
            SingleOutputStreamOperator<Tuple6<String, Double, Double, Double, Double, Integer>> AggregateStream = newTripData1
                    .keyBy(BikesTrip::getStart_station_number)
                    .window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)))
                    .aggregate(new CustomAggregate(locations, 50));

                    //.aggregate(new CustomAggregate(locations, 50));

            AggregateStream.print();

            DataStream<BikesTrip> newTripData = tripDataStream.rebalance();
            //int n = 5; // number of most popular start stations to show
            SingleOutputStreamOperator<Tuple4<String, String, List<String>, List<Integer>>> popularStationsStream = newTripData
                    .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)))
                    .process(new TopNMostPopular(n));

            //popularStationsStream.print();

            CassandraService cassandraService = new CassandraService();
            cassandraService.sinkToCassandraDB(AggregateStream, "tabela", popularStationsStream, "popular_table");

            env.execute();

        }

}



