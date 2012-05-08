package com.asgow.ciel.examples.mapreduce.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Logger {
	
	private DateTime dateTime;
	private String jobType;
	private String jobID;
	private String hostname;
	
	public Logger(String jobID) {
		dateTime = new DateTime();
		jobType = "MapReduce: ";
		this.jobID = jobID;
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			System.out.println("Logger failed to get hostname at: " + dateTime.getCurrentDateTime() 
					+ " for job: " + jobID);
			hostname = "none";
		}
	}
	
	public void LogEvent(String event) {
		System.out.println(jobType + event + " at: " + dateTime.getCurrentDateTime() 
				+ " for job: " + jobID + " on machine: " + hostname);		
	}
	
	public void LogEventTimestamp(String event, long startTime) {
		System.out.println(jobType + event + " in: "
				 + Double.toString((System.currentTimeMillis() - startTime)/1000.0) + " secs at: " 
				 + dateTime.getCurrentDateTime() + " for job: " + jobID + " on machine: " + hostname);
	}
	
}
	