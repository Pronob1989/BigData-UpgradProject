package com.upgrad.saavnproject;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongTrendByDateReducer extends Reducer<SongIdAndPartitionIdKey, HourAndCount, Text, Text> {
	private static final int MAX_HOUR_ARRAY_INDEX = 24;
	private static final int MAX_DAYS_ARRAY_INDEX = 31;
	private int[][] streamWeight = new int[MAX_DAYS_ARRAY_INDEX][MAX_HOUR_ARRAY_INDEX];
	
	public void reduce(SongIdAndPartitionIdKey key, Iterable<HourAndCount> values, Context context) throws IOException, InterruptedException {
		for (int dayIter = 1; dayIter < MAX_DAYS_ARRAY_INDEX; dayIter++) {
			for (int hourIter = 0; hourIter < MAX_HOUR_ARRAY_INDEX; hourIter++) {
				streamWeight[dayIter][hourIter] = 0;
			}
		}
			
		for (HourAndCount val: values) {
			int day = val.getDay();
			int hour = val.getHour();
			int weight = (val.getCount()) * (day * 2 + hour * 4);
			streamWeight[day][hour] = streamWeight[day][hour] + weight;
		}
		
		int count = 0;
		for (int dayIter = 1; dayIter < MAX_DAYS_ARRAY_INDEX; dayIter++) {
			for (int hourIter = 0; hourIter < MAX_HOUR_ARRAY_INDEX; hourIter++) {
				if (streamWeight[dayIter][hourIter] != 0) {
					count = count + streamWeight[dayIter][hourIter];
				}
			}
		}
		if (count != 0) {
			context.write(new Text(key.getSongId()), new Text(Integer.toString(count)));
		}
		
	}
}
