package com.asgow.ciel.examples.mapreduce.wordcount;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.asgow.ciel.examples.mapreduce.common.ReduceTask;

import com.asgow.ciel.references.Reference;

public class WordCountReduce extends ReduceTask {
   
	// 50 MB
	private int spillThreshold = 52428800;
	
	public WordCountReduce(Reference[] input) {
		super(input);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run(DataOutputStream[] dos, DataInputStream dis) {
		try {
			IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(dos, 1, spillThreshold, comb);
			Text word = new Text();
			Text previousWord = new Text();
			IntWritable value = new IntWritable();	
			int sum = 0;	
			
			word.readFields(dis);
			value.readFields(dis);
			
			previousWord = word;
			sum = value.get();
			
			while (true) {								
				try {
					word.readFields(dis);
					value.readFields(dis);
					if(previousWord == word) {
						sum += value.get();
					} else {
						System.out.println(previousWord + " = " + sum);
						outMap.collect(previousWord, new IntWritable(sum));
						previousWord = word;
						sum = value.get();
					}
				} catch (EOFException e) {
					break;
				} catch (RuntimeException e) {
					
				}
			}
			
		} catch (IOException e) {
			System.out.println("IOException while running WordCountReduce");
			e.printStackTrace();
			System.exit(1);
		}

	}

}
