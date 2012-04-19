package com.asgow.ciel.examples.mapreduce.sort;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import com.asgow.ciel.examples.mapreduce.common.MapTask;
import com.asgow.ciel.examples.mapreduce.wordcount.Text;
import com.asgow.ciel.references.Reference;

public class SortMap extends MapTask {

	public SortMap(Reference input, int nReducers) {
		super(input, nReducers);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run(BufferedReader bufferedReader, DataOutputStream[] dos, int numReducers) {
        
        String line;
        try {
			PartialHashOutputCollector<Text, Text> outMap = new PartialHashOutputCollector<Text, Text>(dos, numReducers, 1000);
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
				
        } catch (IOException e) {
			System.out.println("IOException while running SortMap");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
