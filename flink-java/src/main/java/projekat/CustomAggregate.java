package projekat;

import models.BikesTrip;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.*;
import java.util.List;


public class CustomAggregate implements AggregateFunction<BikesTrip, Tuple5<String, Double, Integer, Double, Integer>, Tuple6<String, Double, Double, Double, Double, Integer>> {
    private List<String> locations; // attribute used for filtering by location
    private int threshold; // attribute used for filtering by condition
    private boolean hasLocation;

    public CustomAggregate() {
        this.locations = null;
        this.threshold = 0;
        this.hasLocation = false;
    }

    public CustomAggregate(List<String> locations, int threshold) {
        this.locations = locations;
        this.threshold = threshold;
        this.hasLocation = true;
    }

    public Tuple5<String, Double, Integer, Double, Integer> createAccumulator() {
        return new Tuple5<>("", 0.0, Integer.MAX_VALUE, Double.MIN_VALUE, 0);
    }


    public Tuple5<String, Double, Integer, Double, Integer> add(BikesTrip value, Tuple5<String, Double, Integer, Double, Integer> accumulator) {
        if (hasLocation && !locations.contains(value.getStart_station())) {
            return accumulator;
        }

        if (accumulator.f0.isEmpty()) {
            accumulator.f0 = value.getStart_station();
        }

        int count = accumulator.f4 + 1;
        double total = accumulator.f1 + value.getDuration();
        double avgDuration = total / count;
        int minDuration = Math.min(accumulator.f2, value.getDuration());
        int maxDuration = (int) Math.max(accumulator.f3, value.getDuration());

        if (count >= threshold) {
            return new Tuple5<>(value.getStart_station(), total, minDuration, (double)maxDuration, count);
        } else {
            return new Tuple5<>(value.getStart_station(), total, minDuration, (double)maxDuration, count);
        }
    }

    public Tuple6<String, Double, Double, Double, Double, Integer> getResult(Tuple5<String, Double, Integer, Double, Integer> accumulator) {
        if (!locations.contains(accumulator.f0)) {
            return null;
        }

        double avgDuration = accumulator.f1 / accumulator.f4;
        return new Tuple6<>(accumulator.f0, avgDuration, (double)accumulator.f2, (double)accumulator.f3, accumulator.f1, accumulator.f4);
    }

    public Tuple5<String, Double, Integer, Double, Integer> merge(Tuple5<String, Double, Integer, Double, Integer> a, Tuple5<String, Double, Integer, Double, Integer> b) {
        int count = a.f4 + b.f4;
        double total = a.f1 + b.f1;
        double avgDuration = total / count;
        int minDuration = Math.min(a.f2, b.f2);
        int maxDuration = (int) Math.max(a.f3, b.f3);

        if (count >= threshold) {
            return new Tuple5<>(a.f0, total, minDuration, (double)maxDuration, count);
        } else {
            return new Tuple5<>(a.f0, total, minDuration, (double)maxDuration, count);
        }
    }
}

/*This is a custom implementation of an Apache Flink AggregateFunction that processes BikesTrip objects and produces a
result of type Tuple2<Double, Tuple5<Double, Double, Double, Double, Double>>.
The custom aggregate function has two parameters: location and threshold, which are used to filter the input data by
location and by a threshold value.
The createAccumulator() method initializes an accumulator with five values, set to 0.0, 0.0, the maximum possible double value,
the minimum possible double value, and 0.0.
The add() method takes in a BikesTrip value and an accumulator, and updates the accumulator based on the data in the BikesTrip
object. If the start_station of the trip is not equal to the location parameter, then the accumulator is returned without
modification. Otherwise, the count and total duration of trips are updated, and the minDuration and maxDuration values are
updated based on the current minimum and maximum values and the duration of the current trip. If the count of trips exceeds
the threshold parameter, then the accumulator is returned with the start_station_number, total duration, minimum duration,
maximum duration, and count of trips. Otherwise, the accumulator is returned with only the start_station_number,
total duration, minimum duration, maximum duration, and count of trips.
The getResult() method takes in the final accumulator and returns a Tuple2 object, consisting of the start_station_number
and a Tuple5 object that contains the average duration, minimum duration, maximum duration, total duration, and count of trips.
The merge() method takes in two accumulators and merges them into a single accumulator. It updates the count
and total duration of trips, and updates the minDuration and maxDuration values based on the minimum and
maximum values of the two input accumulators. If the count of trips exceeds the threshold parameter,
then the accumulator is returned with the start_station_number, total duration, minimum duration,
maximum duration, and count of trips. Otherwise, the accumulator is returned with only the start_station_number,
total duration, minimum duration, maximum duration, and count of trips.*/