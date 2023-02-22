package projekat;
import models.BikesTrip;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.*;
import java.util.*;
public class proces extends ProcessAllWindowFunction<Tuple6<String, Double, Double, Double, Double, Integer>, Tuple8<String, String, String, Double, Double, Double, Double, Integer>, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<Tuple6<String, Double, Double, Double, Double, Integer>, Tuple8<String, String, String, Double, Double, Double, Double, Integer>, TimeWindow>.Context context, Iterable<Tuple6<String, Double, Double, Double, Double, Integer>> elements, Collector<Tuple8<String, String, String, Double, Double, Double, Double, Integer>> out) throws Exception {
        String startDateTimeString = LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()), ZoneId.systemDefault()).toString();
        String endDateTimeString = LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()), ZoneId.systemDefault()).toString();

        for (Tuple6<String, Double, Double, Double, Double, Integer> element : elements) {
            String startStationNumber = element.f0;
            Double duration = element.f1;
            Double distance = element.f2;
            Double pickups = element.f3;
            Double dropoffs = element.f4;
            Integer count = element.f5;

            Tuple8<String, String, String, Double, Double, Double, Double, Integer> outputTuple = new Tuple8<>(startDateTimeString, endDateTimeString, startStationNumber, duration, distance, pickups, dropoffs, count);

            out.collect(outputTuple);
        }
    }
}