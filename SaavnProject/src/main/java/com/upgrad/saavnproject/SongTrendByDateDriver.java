package com.upgrad.saavnproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class SongTrendByDateDriver extends Configured implements Tool {

	static final int INPUT_ARG_LENGTH = 2;
	
	static final int NUM_REDUCE_TASKS = 7;
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		BasicConfigurator.configure();
		int status = ToolRunner.run(new Configuration(), new SongTrendByDateDriver(), args);
		System.exit(status);

	}
	
	

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length != INPUT_ARG_LENGTH) {
			System.err.println("Usage: SongTrendByDateDriver <input path> <MR output path>");
			System.err.println("Input format req : (song ID, user ID, timestamp, hour, date)");
		    System.exit(-1);
		}
		
		Job job1 = new Job(getConf(), "SongTrendByDate1");
		
		job1.setJarByClass(SongTrendByDateDriver.class);
		
		job1.setMapperClass(SongTrendByDateMapper.class);
		job1.setReducerClass(SongTrendByDateReducer.class);
		job1.setCombinerClass(SongTrendByDateCombiner.class);
		job1.setPartitionerClass(SongTrendByDatePartitioner.class);
		
		job1.setNumReduceTasks(NUM_REDUCE_TASKS);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.setMapOutputKeyClass(SongIdAndPartitionIdKey.class);
		job1.setMapOutputValueClass(HourAndCount.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		boolean status = job1.waitForCompletion(true);
		if (!status) {
			System.err.println("Job: "+ job1.getJobName() + " failed to complete!!!!");
		    System.exit(-1);
		}
		return 0;
	}

}
