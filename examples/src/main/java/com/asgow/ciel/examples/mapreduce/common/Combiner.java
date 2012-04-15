package com.asgow.ciel.examples.mapreduce.common;
public interface Combiner<V> {
	
	public V combine(V oldValue, V newValue);
	
}
	