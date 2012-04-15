package com.asgow.ciel.examples.mapreduce.wordcount;
public interface Combiner<V> {
	
	public V combine(V oldValue, V newValue);
	
}
	