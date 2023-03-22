package projekat;

import models.BikesTrip;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.*;
import java.util.*;


public class TopNMostPopular extends ProcessAllWindowFunction<BikesTrip, Tuple4<String, String, List<String>, List<Integer>>, TimeWindow> {
    private final int n;
    public TopNMostPopular(int n) {
        this.n = n;
    }

    @Override
    public void process(Context context, Iterable<BikesTrip> input, Collector<Tuple4<String, String, List<String>, List<Integer>>> out) {
        Map<String, Integer> startStationCounts = new HashMap<>();

        // Count the number of occurrences for each start station
        for (BikesTrip trip : input) {
            String startStation = trip.getStart_station();
            int count = startStationCounts.getOrDefault(startStation, 0) + 1;
            startStationCounts.put(startStation, count);
        }
        // Sort the start stations by their counts in descending order
        List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(startStationCounts.entrySet());
        sortedEntries.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));

        // Output the top N most popular start stations along with their respective counts and window timestamp
        List<String> stations = new ArrayList<>();
        List<Integer> numbers = new ArrayList<>();
        for (int i = 0; i < n && i < sortedEntries.size(); i++) {
            stations.add(sortedEntries.get(i).getKey());
            numbers.add(sortedEntries.get(i).getValue());
        }
        String startDateTimeString = LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()), ZoneId.systemDefault()).toString();
        String endDateTimeString = LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()), ZoneId.systemDefault()).toString();

        out.collect(new Tuple4<>(startDateTimeString, endDateTimeString, stations, numbers));
    }
}

