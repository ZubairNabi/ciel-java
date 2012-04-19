package com.asgow.ciel.examples.mapreduce.sort;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.asgow.ciel.examples.mapreduce.common.ReduceTask;
import com.asgow.ciel.examples.mapreduce.wordcount.Text;

import com.asgow.ciel.references.Reference;

public class SortReduce extends ReduceTask {
	
	// 50 MB
	private int spillThreshold = 52428800;
    
	public SortReduce(Reference[] input) {
		super(input);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run(DataOutputStream[] dos, DataInputStream dis) {
		try {
			PartialHashOutputCollector<Text, Text> outMap = new PartialHashOutputCollector<Text, Text>(dos, 1, spillThreshold);			
			while (true) {			
				Text word = new Text();
				Text value = new Text();
				try {
					word.readFields(dis);
					value.readFields(dis);
					System.out.println(word + " = " + value);
					outMap.collect(word, value);
				} catch (EOFException e) {
					break;
				} catch (RuntimeException e) {
					break;
				} 
			}
			
			//flush all key, values to collector, close the data stream, and delete the temp file
			outMap.flushAll();
						
		} catch (IOException e) {
			System.out.println("IOException while running SortReduce");
			e.printStackTrace();
			System.exit(1);
		}
	
	}

}
