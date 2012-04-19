package com.asgow.ciel.examples.mapreduce.wordcount;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;


import com.asgow.ciel.examples.mapreduce.common.MapTask;
import com.asgow.ciel.references.Reference;

public class WordCountMap extends MapTask {
	
	private final static IntWritable one = new IntWritable(1);
	// 50 MB
	private int spillThreshold = 52428800;
   
    public WordCountMap(Reference input, int nReducers) {
		super(input, nReducers);
		// TODO Auto-generated constructor stub
	}

    @Override
	public void run(BufferedReader bufferedReader, DataOutputStream[] dos, int numReducers) {     
        String line;
        try {
        	IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(dos, numReducers, spillThreshold, comb);
			while ((line = bufferedReader.readLine()) != null) { 
				//System.out.println(line);
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					Text word = new Text();
					word.set(itr.nextToken());
					outMap.collect(word, one);
				}
			}
			outMap.flushAll();			
		} catch (IOException e) {
			System.out.println("IOException while running WordCountMap");
			e.printStackTrace();
			System.exit(1);
		}

	}
	
}
