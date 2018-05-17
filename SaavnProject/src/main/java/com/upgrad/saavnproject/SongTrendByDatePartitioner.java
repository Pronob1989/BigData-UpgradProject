package com.upgrad.saavnproject;

import org.apache.hadoop.mapreduce.Partitioner;

public class SongTrendByDatePartitioner extends Partitioner<SongIdAndPartitionIdKey,HourAndCount> {
	@Override
	public int getPartition(SongIdAndPartitionIdKey key, HourAndCount value, int numPartitions) {
		return (key.getPartitionId());
	}
}
