package com.asgow.ciel.examples.mapreduce.common;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTime implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2052568223631842589L;
	private DateFormat dateFormat;
	private Date date;
	
	public DateTime () {
		date = new Date();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	}
	
	public String getCurrentDateTime() {
		return this.dateFormat.format(date);
	}

}
	