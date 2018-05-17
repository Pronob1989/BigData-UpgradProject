package com.upgrad.saavnproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class HourAndCount implements WritableComparable<HourAndCount> {
	int day;
	int hour;
	int count;

	public HourAndCount() {super();}

	public HourAndCount(int day, int hour, int count) {
		this.day = day;
	    this.hour = hour;
	    this.count = count;
	}

	public int getDay() {return day;}
	public void setDay(int day) {this.day = day;}
	public int getHour() {return hour;}
	public void setHour(int hour) {this.hour = hour;}
	public int getCount() {return count;}
	public void setCount(int count) {this.count = count;}

	
	public void readFields(DataInput dataInput) throws IOException {
		day = WritableUtils.readVInt(dataInput);
	    hour = WritableUtils.readVInt(dataInput);
	    count = WritableUtils.readVInt(dataInput);      
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, day);
	    WritableUtils.writeVInt(dataOutput, hour);
	    WritableUtils.writeVInt(dataOutput, count);
	}

	public boolean equals(Object o) {
		if (o instanceof HourAndCount) {
			HourAndCount ds = (HourAndCount) o;
			return ((day == ds.day) && (hour == ds.hour));
		}
		return false;
	
	}
	
	public int hashCode() {
		return (day * 24 + hour);
	}
	
	public int compareTo(HourAndCount o) {
		int cmp = Integer.compare(day, o.day);
		if (cmp!=0) {
			return cmp;
		}
		cmp = Integer.compare(hour, o.hour);
		return cmp;
	}
	public String toString() {
		return Integer.toString(day) + " " + Integer.toString(hour) + " " + Integer.toString(count);
 		
	}

}