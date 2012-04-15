package com.asgow.ciel.examples.mapreduce.sort;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.StringTokenizer;

import com.asgow.ciel.examples.mapreduce.wordcount.Text;
import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.std.TextFileSorter;

public class SortMap implements ConstantNumOutputsTask {

    private Reference input;
    private int nReducers;
	
	public SortMap(Reference input, int nReducers) {
		this.input = input;
		this.nReducers = nReducers;
	}
	
	public int getNumOutputs() {
		return this.nReducers;
	}
	
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	public void invoke() throws Exception {
        System.out.println("SortMap started at " + System.currentTimeMillis());
        
        // convert input reference to stream and then create a BufferedReader
        InputStream inputStream = Ciel.RPC.getStreamForReference(this.input);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        
        // number of output files would be equal to number of reducers, so creating that many outputstreams and references
        OutputStream[] outputs = new OutputStream[nReducers];
        WritableReference[] resultReference = new WritableReference[nReducers];
        
        // create temp files to store unsorted results
        File tempFiles[] = new File[nReducers];
        OutputStream[] tempOutputs = new OutputStream[nReducers];
        DataOutputStream[] tempDos = new DataOutputStream[nReducers];
        		       
        for(int i = 0; i < nReducers; i++) {
        	// create temp files and output streams
        	tempFiles[i] = File.createTempFile("reduce_" + Integer.toString(i) , ".tmp");
        	tempOutputs[i] = new FileOutputStream(tempFiles[i]);
			tempDos[i] = new DataOutputStream(new BufferedOutputStream(tempOutputs[i]));
			
			// get references for output files and convert to OutputStream
			resultReference[i] = Ciel.RPC.getOutputFilename(i);
        	outputs[i] = resultReference[i].open();
		}

        String line;
        Text blank = new Text("");
        try {
			PartialHashOutputCollector<Text, Text> outMap = new PartialHashOutputCollector<Text, Text>(tempDos, nReducers, 1000);
			while ((line = bufferedReader.readLine()) != null) { 
				//System.out.println(line);
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					Text word = new Text();
					word.set(itr.nextToken());
					outMap.collect(word, blank);
				}
			}
			outMap.flushAll();
			
			// now sort the temp files with 50 Mb mem limit
			TextFileSorter sorter = new TextFileSorter(new SortConfig().withMaxMemoryUsage(50 * 1000 * 1000));
			for(int i = 0; i < nReducers; i++) {
				sorter.sort(new FileInputStream(tempFiles[i]), outputs[i]);
		        // delete temp files
		        tempFiles[i].delete();
		        // close output stream
		        outputs[i].close();
			}
			
			// close output streams
			for (DataOutputStream d : tempDos) 
				d.close();

		} catch (IOException e) {
			System.out.println("IOException while running SortMap");
			e.printStackTrace();
			System.exit(1);
		}

        System.out.println("WordCountMap finished at " + System.currentTimeMillis());
	}

	public void setup() {

	}

}
