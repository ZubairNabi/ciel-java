package com.ciel.mapreduce.wordcount;
public interface Combiner<V> {
	
	public V combine(V oldValue, V newValue);
	
}
	