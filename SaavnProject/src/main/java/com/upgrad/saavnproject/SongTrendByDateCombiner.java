package com.upgrad.saavnproject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongTrendByDateCombiner extends Reducer<SongIdAndPartitionIdKey, HourAndCount, SongIdAndPartitionIdKey, HourAndCount>{
	private static final int MAX_HOUR_ARRAY_INDEX = 24;
	private static final int MAX_DAYS_ARRAY_INDEX = 31;
	private int[][] streamCount = new int[MAX_DAYS_ARRAY_INDEX][MAX_HOUR_ARRAY_INDEX];
	
	public void reduce(SongIdAndPartitionIdKey key, Iterable<HourAndCount> values, Context context) throws IOException, InterruptedException {
		for (int dayIter = 1; dayIter < MAX_DAYS_ARRAY_INDEX; dayIter++) {
			for (int hourIter = 0; hourIter < MAX_HOUR_ARRAY_INDEX; hourIter++) {
				streamCount[dayIter][hourIter] = 0;
			}
		}
			
		for (HourAndCount val: values) {
			int day = val.getDay();
			int hour = val.getHour();
			streamCount[day][hour] = streamCount[day][hour] + val.getCount(); 
		}
		for (int dayIter = 1; dayIter < MAX_DAYS_ARRAY_INDEX; dayIter++) {
			for (int hourIter = 0; hourIter < MAX_HOUR_ARRAY_INDEX; hourIter++) {
				if (streamCount[dayIter][hourIter] != 0) {
					context.write(key, new HourAndCount(dayIter, hourIter,streamCount[dayIter][hourIter]));
				}
			}
		}
	}
}
