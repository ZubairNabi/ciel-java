package com.asgow.ciel.examples.mapreduce.sort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.asgow.ciel.examples.mapreduce.common.MergeFiles;
import com.asgow.ciel.examples.mapreduce.wordcount.*;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class SortReduce implements ConstantNumOutputsTask {
        
    private Reference[] input;
	
	public SortReduce(Reference[] input) {
		this.input = input;
	}
	
	public Reference[] getDependencies() {
		return this.input;
	}
	
	public int getNumOutputs() {
		return 1;
	}

	public void invoke() throws Exception {
        System.out.println("SortReduce started");
        int nInputs = input.length;
		InputStream[] is = new InputStream[nInputs];
		DataOutputStream[] dos = new DataOutputStream[1];
        OutputStream[] outputs = new OutputStream[1];
        List<InputStream> listStreams = new ArrayList<InputStream>();
		
		for(int i = 0; i < nInputs; i++) {
			is[i] = new BufferedInputStream(Ciel.RPC.getStreamForReference(this.input[i]));
			listStreams.add(i, is[i]);
		}
        
        File tempFile = File.createTempFile("reduce_" + Integer.toString(nInputs) , ".tmp");
        FileOutputStream tempOutput = new FileOutputStream(tempFile);
        
        MergeFiles mergeFiles = new MergeFiles();
        mergeFiles.mergeFiles(listStreams, tempOutput);
				
		WritableReference resultReference = Ciel.RPC.getOutputFilename(0);	
        outputs[0] = resultReference.open();
		dos[0] = new DataOutputStream(new BufferedOutputStream(outputs[0]));
		
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));

		try {
			IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(dos, 1, Integer.MAX_VALUE, comb);
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
			
			//flush all key, values to collector, close the data stream, and delete the temp file
			outMap.flushAll();
			dos[0].close();			
			tempFile.delete();
			
		} catch (IOException e) {
			System.out.println("IOException while running SortReduce");
			e.printStackTrace();
			System.exit(1);
		}

        System.out.println("SortReduce finished");
		Ciel.returnPlainString("SortReduce finished and wrote to file " + resultReference.getFilename());
	}

	public void setup() {
		
	}

}
