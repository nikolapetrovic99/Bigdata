package models;

import java.sql.Timestamp;
import lombok.*;
@Builder
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class BikesTrip {
    private int duration;
    private String start_date;
    private String end_date;
    private int start_station_number;
    private String start_station;
    private int end_station_number;
    private String end_station;
    private String bike_number;
    private String member_type;


    public BikesTrip() {
        this.duration = 0;
        this.start_date = "";
        this.end_date = "";
        this.start_station_number =0;
        this.start_station = "";
        this.end_station_number = 0;
        this.end_station = "";
        this.bike_number = "";
        this.member_type = "";

    }
    public BikesTrip(int duration, String start_date, String end_date, int start_station_number,
                     String start_station, int end_station_number, String end_station,
                     String bike_number, String member_type) {
        this.duration = duration;
        this.start_date = start_date;
        this.end_date = end_date;
        this.start_station_number = start_station_number;
        this.start_station = start_station;
        this.end_station_number = end_station_number;
        this.end_station = end_station;
        this.bike_number = bike_number;
        this.member_type = member_type;
    }


    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getStart_date() {
        return start_date;
    }

    public void setStart_date(String start_date) {
        this.start_date = start_date;
    }

    public String getEnd_date() {
        return end_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }

    public int getStart_station_number() {
        return start_station_number;
    }

    public void setStart_station_number(int start_station_number) {
        this.start_station_number = start_station_number;
    }

    public String getStart_station() {
        return start_station;
    }

    public void setStart_station(String start_station) {
        this.start_station = start_station;
    }

    public int getEnd_station_number() {
        return end_station_number;
    }

    public void setEnd_station_number(int end_station_number) {
        this.end_station_number = end_station_number;
    }

    public String getEnd_station() {
        return end_station;
    }

    public void setEnd_station(String end_station) {
        this.end_station = end_station;
    }

    public String getBike_number() {
        return bike_number;
    }

    public void setBike_number(String bike_number) {
        this.bike_number = bike_number;
    }

    public String getMember_type() {
        return member_type;
    }

    public void setMember_type(String member_type) {
        this.member_type = member_type;
    }


}
