package com.upgrad.saavnproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SongIdAndPartitionIdKey implements WritableComparable<SongIdAndPartitionIdKey> {
	int pId;
	String songId;

	public SongIdAndPartitionIdKey() {super();}

	public SongIdAndPartitionIdKey(String songId, int pId) {
	    this.pId = pId;
	    this.songId = songId;
	}

	public int getPartitionId() {return pId;}
	public void setPartitionId(int pId) {this.pId = pId;}
	public String getSongId() {return songId;}
	public void setSongId(String songId) {this.songId = songId;}

	
	public void readFields(DataInput dataInput) throws IOException {
	    pId = WritableUtils.readVInt(dataInput);
	    songId = WritableUtils.readString(dataInput);      
	}

	public void write(DataOutput dataOutput) throws IOException {
	    WritableUtils.writeVInt(dataOutput, pId);
	    WritableUtils.writeString(dataOutput, songId);
	}

	public int hashCode() {
		return  songId.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof SongIdAndPartitionIdKey) {
			SongIdAndPartitionIdKey ds = (SongIdAndPartitionIdKey) o;
			return ((pId == ds.getPartitionId()) && (songId.equals(ds.getSongId())));
		}
		return false;
	}
	public int compareTo(SongIdAndPartitionIdKey o) {
		int cmp = Integer.compare(pId, o.getPartitionId());
		if (cmp!=0) {
			return cmp;
		}
		return songId.compareTo(o.getSongId());
	}
	public String toString() {
		return Integer.toString(pId) + " " + songId;
 		
	}

}
