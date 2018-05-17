package com.upgrad.saavnproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class DateAndSongId implements WritableComparable<DateAndSongId> {
	int day;
	String songId;

	public DateAndSongId() {super();}

	public DateAndSongId(int day, String songId) {
	    this.day = day;
	    this.songId = songId;
	}

	public int getDay() {return day;}
	public void setDay(int day) {this.day = day;}
	public String getSongId() {return songId;}
	public void setSongId(String songId) {this.songId = songId;}

	
	public void readFields(DataInput dataInput) throws IOException {
	    day = WritableUtils.readVInt(dataInput);
	    songId = WritableUtils.readString(dataInput);      
	}

	public void write(DataOutput dataOutput) throws IOException {
	    WritableUtils.writeVInt(dataOutput, day);
	    WritableUtils.writeString(dataOutput, songId);
	}

	public int hashCode() {
		return  songId.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof DateAndSongId) {
			DateAndSongId ds = (DateAndSongId) o;
			return ((day == ds.day) && (songId.equals(ds.getSongId())));
		}
		return false;
	}
	public int compareTo(DateAndSongId o) {
		int cmp = Integer.compare(day, o.day);
		if (cmp!=0) {
			return cmp;
		}
		return songId.compareTo(o.getSongId());
	}

}

