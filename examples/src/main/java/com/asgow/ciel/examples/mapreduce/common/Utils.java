package com.asgow.ciel.examples.mapreduce.common;

import java.io.InputStream;
import java.io.OutputStream;

public class Utils {
	
	public static long checkMemory() {
		return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
	}
	
	public static void closeOutputStream(OutputStream outputStream) {
		try {
			if (outputStream != null) {
				outputStream.close();
			}
		} catch(Exception e) {
			System.out.println("Exception while closing output stream");
			e.printStackTrace();
		}
	}
	
	public static void closeInputStream(InputStream inputStream) {
		try {
			if (inputStream != null) {
				inputStream.close();
			}
		} catch(Exception e) {
			System.out.println("Exception while closing input stream");
			e.printStackTrace();
		}
	}
}
