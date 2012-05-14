package com.asgow.ciel.examples.mapreduce.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Logger {
	
	private DateTime dateTime;
	private String jobType;
	private String jobID;
	private String hostname;
	private String separator;
	
	public static String STARTED = "started";
	public static String ASSIGNED_INPUT = "assigned input reference: ";
	public static String FETCHED_INPUT = "input fetch completed";
	public static String LOGIC_FINISHED = "logic completed";
	public static String OUTPUT_SIZE = "total size of output files: ";
	public static String MERGE_SIZE = "merged file size: ";
	public static String MERGE_FINISHED = "merge completed";
	public static String SORT_FINISHED = "sort completed";
	public static String PRODUCED_OUTPUT = "produced output reference: ";
	public static String FINISHED = "finished";
	public static String EXCEPTION = "exception";
	
	public Logger(String jobID) {
		dateTime = new DateTime();
		jobType = "MapReduce";
		separator = "|";
		this.jobID = jobID;
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			System.out.println("Logger failed to get hostname at: " + dateTime.getCurrentDateTime() 
					+ " for job: " + jobID);
			hostname = "none";
		}
	}
	
	public void LogEvent(String taskID, String event, long startTime) {
		String timeTaken = "0";
		if(startTime != 0) {
			timeTaken = Double.toString((System.currentTimeMillis() - startTime)/1000.0);
		}
		System.out.println(jobType + separator
				+ taskID + separator
				+ event + separator
				+ timeTaken + separator
				+ dateTime.getCurrentDateTime() + separator
				+ hostname + separator
				+ jobID
			);
	}
	
}
	