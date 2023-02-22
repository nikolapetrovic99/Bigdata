package projekat;

import com.datastax.driver.core.*;
import models.BikesTrip;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

import java.util.List;


/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */
public final class CassandraService {
    private static final String KEYSPACE_NAME = "newkeyspace";
    private static final String TABLE_NAME = "rides";


    public final void sinkToCassandraDB(SingleOutputStreamOperator<Tuple6<String, Double, Double, Double, Double, Integer>> sinkTripsStream, String table, SingleOutputStreamOperator<Tuple4<String, String, List<String>, List<Integer>>> popularStationsStream, String table2) throws Exception {


       /* SingleOutputStreamOperator<Tuple3<String, String, String>> sinkTripsDataStream = sinkTripsStream.map((MapFunction<BikesTrip, Tuple3<String, String, String>>) trip ->
                        new Tuple3<>(trip.getStart_station(), trip.getEnd_station(), trip.getBike_number()))
                .returns(new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class)));
        */
        try (Cluster cluster = Cluster.builder().addContactPoint("cassandra"/*app na kontejneru*//*"127.0.0.1"*//*app na lokalu*/).build()) {
            Session session = cluster.connect();

            ResultSet rs = session.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '" + KEYSPACE_NAME + "'");
            if (rs.isExhausted()) {
                session.execute("CREATE KEYSPACE " + KEYSPACE_NAME + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            }
            rs = session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '" + KEYSPACE_NAME + "' AND table_name = '" + table + "'");
            if (rs.isExhausted()) {
                //treba da se doda primary key kad se ubaci vreme da to bude komb
                session.execute("CREATE TABLE " + KEYSPACE_NAME + "." + "\"" + table + "\"" + " (start_station text PRIMARY KEY, avg_duration double, min_duration double, max_duration double, total_duration double, count int)");
            }
            rs = session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '" + KEYSPACE_NAME + "' AND table_name = '" + table2 + "'");
            if (rs.isExhausted()) {
                session.execute("CREATE TABLE " + KEYSPACE_NAME + "." + "\"" + table2 + "\"" + " (window_start text, window_end text, stations list<text>, counts list<int>, PRIMARY KEY ((window_start, window_end)))");
            }


        }
        //"Open Cassandra connection and Sinking data into cassandraDB."
        CassandraSink.addSink(sinkTripsStream)
                //.setHost("127.0.0.1")/*app na lokalu*/
                .setHost("cassandra")/*app na kontejneru*/
                .setQuery("INSERT INTO "+KEYSPACE_NAME+"."+"\"" + table + "\""+"(start_station, avg_duration, min_duration, max_duration, total_duration, count) VALUES (?, ?, ?, ?, ?, ?);")
                .build();
        CassandraSink.addSink(popularStationsStream)
                //.setHost("127.0.0.1")/*app na lokalu*/
                .setHost("cassandra")/*app na kontejneru*/
                .setQuery("INSERT INTO "+KEYSPACE_NAME+"."+"\"" + table2 + "\""+"(window_start, window_end, stations, counts) VALUES (?, ?, ?, ?);")
                .build();

    }
}
