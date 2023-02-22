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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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

	public static void main(String[] args) throws Exception {

			String inputTopic = "locations";
			String server = "kafka:9092";

			StramConsumrer(inputTopic, server);
		}

		public static void StramConsumrer(String inputTopic, String server) throws Exception {
			StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
			FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
			DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);


			stringInputStream.map(new MapFunction<String, String>() {
				private static final long serialVersionUID = -999736771747691234L;

				@Override
				public String map(String value) throws Exception {
					return "Receiving from Kafka : " + value;
				}
			}).print();
			System.out.println( "Hello World!" );
			environment.execute();
		}

		public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
				String topic, String kafkaAddress) {

			Properties props = new Properties();
			props.setProperty("bootstrap.servers", kafkaAddress);
			//props.setProperty("group.id",kafkaGroup);
			FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
					topic, new SimpleStringSchema(), props);

			return consumer;
		}
























		//		// Sets up the execution environment, which is the main entry point
//		// to building Flink applications.
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		/*
//		 * Here, you can start creating your execution plan for Flink.
//		 *
//		 * Start with getting some data from the environment, like
//		 * 	env.fromSequence(1, 10);
//		 *
//		 * then, transform the resulting DataStream<Long> using operations
//		 * like
//		 * 	.filter()
//		 * 	.flatMap()
//		 * 	.window()
//		 * 	.process()
//		 *
//		 * and many more.
//		 * Have a look at the programming guide:
//		 *
//		 * https://nightlies.apache.org/flink/flink-docs-stable/
//		 *
//		 */
//
//		// Execute program, beginning computation.
//		env.execute("Flink Java API Skeleton");
	}
