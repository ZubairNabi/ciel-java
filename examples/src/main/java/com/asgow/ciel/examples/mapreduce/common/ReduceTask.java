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
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class ReduceTask implements ConstantNumOutputsTask {
        
    private Reference[] input;
    private DateTime dateTime;
    private int id;
    private String jobID;
	
	public ReduceTask(Reference[] input, int id, String jobID) {
		this.input = input;
		dateTime = new DateTime();
		this.id = id;
		this.jobID = jobID;
	}
	
	public Reference[] getDependencies() {
		return this.input;
	}
	
	public int getNumOutputs() {
		return 1;
	}

	public void invoke() throws Exception {
		long startTime = System.currentTimeMillis();
        System.out.println("MapReduce: Reduce " + Integer.toString(id) + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
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
	        
	        System.out.println("MapReduce: Reduce " + Integer.toString(id) + " merge completed in "
	       		 + Double.toString((System.currentTimeMillis() - startTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
			
	        // create output file reference and get outputstream	
	        WritableReference writableReference = Ciel.RPC.getOutputFilename(0);
			dos[0] = new DataOutputStream(new BufferedOutputStream(writableReference.open()));

			// create input stream for the single sorted input file
	        dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
	        
	        //run reduce logic
	        run(dos, dis);
        } catch (Exception e) {
        	System.out.println("MapReduce: Exception while running ReduceTask" + " for job: " + jobID);
       	 	e.printStackTrace();
       } finally {
       		// close input streams
    		listStreams.clear();
    		Utils.closeInputStream(dis);
    		
        	// close output stream and delete temp file
    		tempFile.delete();
    		Utils.closeOutputStream(tempOutput);
    		dos[0].flush();
    		Utils.closeOutputStream(dos[0]);	
        }
        
        System.out.println("MapReduce: Reduce " + Integer.toString(id) + " finished in "
		 + Double.toString((System.currentTimeMillis() - startTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);		
	}

	public void setup() {
		
	}
	
	public void run(DataOutputStream[] dos, DataInputStream dis) throws Exception {
		
	}

}
