package com.upgrad.saavnproject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongTrendByDateMapper extends Mapper<Object, Text, SongIdAndPartitionIdKey, HourAndCount> {

	/* stream input format - songid, userid, unixtimestamp, hour, date (yyyy-month-day) */
	static final int STREAM_INFO_NUM_FIELDS = 5;
	
	enum StreamInfoInvalid {
		INVALID_NUM_FIELDS,
		INVALID_DAY_FIELD,
		INVALID_HOUR_FIELD,
		INVALID_SONG_FIELD
	}
	

	private static final Map<Integer, List<Integer>> dateToPartitionMap;
	static {
		dateToPartitionMap = new HashMap<Integer, List<Integer>>();
		dateToPartitionMap.put(0, Arrays.asList(0,6));
		dateToPartitionMap.put(25, Arrays.asList(1,6));
		dateToPartitionMap.put(26, Arrays.asList(2,6));
		dateToPartitionMap.put(27, Arrays.asList(3,6));
		dateToPartitionMap.put(28, Arrays.asList(4,6));
		dateToPartitionMap.put(29, Arrays.asList(5,6));
		dateToPartitionMap.put(30, Arrays.asList(6,6));
	}
	
	private void writeForMultiplePartitions(
			int pStart,int pEnd, Context context, String songId, HourAndCount vObj) throws IOException, InterruptedException {
		for (int pId = pStart; pId <= pEnd; pId++) {
			context.write(new SongIdAndPartitionIdKey(songId, pId), vObj);
		}
	}
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] streamInfo = value.toString().split(",");
		
		if (streamInfo.length != STREAM_INFO_NUM_FIELDS) {
			context.setStatus("Detected possibly invalid record: see logs." );
			context.getCounter(StreamInfoInvalid.INVALID_NUM_FIELDS).increment(1);
		    return;
		}
		
		if (streamInfo[0].equals("(null)")) {
			context.getCounter(StreamInfoInvalid.INVALID_SONG_FIELD).increment(1);
		    return;
		}
		
		String[] date = streamInfo[4].split("-");
		int day = Integer.parseInt(date[2]);
		if (day < 1 || day > 30) {
			if (day == 31) {
				return;
			}
			context.setStatus("Detected possibly invalid  record: see logs.");
			context.getCounter(StreamInfoInvalid.INVALID_DAY_FIELD).increment(1);
		    return;
		}
		
		int hour = Integer.parseInt(streamInfo[3]);
		if (hour < 0 || hour > 23) {
			context.setStatus("Detected possibly invalid record: see logs." );
			context.getCounter(StreamInfoInvalid.INVALID_HOUR_FIELD).increment(1);
		    return;
		}
		
		int dateKey = 0;
		
		if (day < 25) {
			dateKey = 0;
		} else {
			dateKey = day;
		}
		
		List<Integer> pRange = dateToPartitionMap.get(dateKey);
		writeForMultiplePartitions(
				pRange.get(0), pRange.get(1), context, streamInfo[0], new HourAndCount(day, hour, 1));
	}
}
