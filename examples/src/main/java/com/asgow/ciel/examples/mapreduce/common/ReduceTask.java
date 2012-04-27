package com.asgow.ciel.examples.mapreduce.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.asgow.ciel.examples.mapreduce.common.MergeFiles;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class ReduceTask implements ConstantNumOutputsTask {
        
    private Reference[] input;
    private DateTime dateTime;
	
	public ReduceTask(Reference[] input) {
		this.input = input;
		dateTime = new DateTime();
	}
	
	public Reference[] getDependencies() {
		return this.input;
	}
	
	public int getNumOutputs() {
		return 1;
	}

	public void invoke() throws Exception {
        System.out.println("Reduce started at " + dateTime.getCurrentDateTime());
        int nInputs = input.length;
		DataOutputStream[] dos = new DataOutputStream[1];
        List<InputStream> listStreams = new ArrayList<InputStream>();
		
		for(int i = 0; i < nInputs; i++) {
			listStreams.add(i, new BufferedInputStream(Ciel.RPC.getStreamForReference(this.input[i])));
		}
        
		// create temporary file for storing results of merge 
        File tempFile = File.createTempFile("reduce_" + Integer.toString(nInputs) , ".tmp");
        FileOutputStream tempOutput = new FileOutputStream(tempFile);
        
        DataInputStream dis = null;
        
        // merge all input files into one sorted one
        MergeFiles mergeFiles = new MergeFiles();
        try {
	        mergeFiles.mergeFiles(listStreams, tempOutput);
	        tempOutput.flush();
			
	        // create output file reference and get outputstream	
			dos[0] = new DataOutputStream(new BufferedOutputStream(Ciel.RPC.getOutputFilename(0).open()));

			// create input stream for the single sorted input file
	        dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
	        
	        //run reduce logic
	        run(dos, dis);
        } finally {
       		// close input streams
    		listStreams.clear();
    		dis.close();
    		
        	// close output stream and delete temp file
    		tempFile.delete();
    		tempOutput.close();
    		dos[0].flush();
    		dos[0].close();			
        }
        
        System.out.println("Reduce finished at " + dateTime.getCurrentDateTime());		
	}

	public void setup() {
		
	}
	
	public void run(DataOutputStream[] dos, DataInputStream dis) {
		
	}

}
