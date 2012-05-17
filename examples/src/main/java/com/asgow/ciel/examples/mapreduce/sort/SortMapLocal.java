package com.asgow.ciel.examples.mapreduce.sort;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.util.StringTokenizer;

import com.asgow.ciel.examples.mapreduce.common.MapTaskLocal;
import com.asgow.ciel.examples.mapreduce.wordcount.Text;

public class SortMapLocal extends MapTaskLocal {
	
	private int spillThreshold = 100;

	public SortMapLocal(String input, int nReducers, int id, String jobID) {
		super(input, nReducers, id, jobID);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run(BufferedReader bufferedReader, DataOutputStream[] dos, int numReducers) throws Exception {
        
        String line;
		PartialHashOutputCollector<Text, Text> outMap = new PartialHashOutputCollector<Text, Text>(dos, numReducers, spillThreshold);
		while ((line = bufferedReader.readLine()) != null) { 
			//System.out.println(line);
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				Text word = new Text();
		        Text blank = new Text("");
				word.set(itr.nextToken());
				outMap.collect(word, blank);
			}
		}
		outMap.flushAll();
	}
}
