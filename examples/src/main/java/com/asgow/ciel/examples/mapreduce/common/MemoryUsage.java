package com.asgow.ciel.examples.mapreduce.common;

public class MemoryUsage {
	public static long checkMemory() {
		return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
	}
}
