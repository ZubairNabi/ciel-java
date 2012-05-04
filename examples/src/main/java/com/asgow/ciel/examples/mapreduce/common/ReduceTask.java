package com.asgow.ciel.examples.mapreduce.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import com.asgow.ciel.examples.mapreduce.terasort.SWTeraMerger;

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
		Ciel.log("MapReduce: Reduce " + Integer.toString(id) + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        System.out.println("MapReduce: Reduce " + Integer.toString(id) + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        
        // create input references
        int nInputs = input.length;
		DataOutputStream[] dos = new DataOutputStream[1];
        InputStream[] inputs = new InputStream[nInputs];
		for(int i = 0; i < nInputs; i++) {
			inputs[i] = Ciel.RPC.getStreamForReference(this.input[i]);
			System.out.println("MapReduce: Reduce " + Integer.toString(id) + " assigned input reference: " 
			        + this.input[i].toJson().get("__ref__").toString() + " at " 
			        + dateTime.getCurrentDateTime() + " for job: " + jobID);
			inputs[i] = Ciel.RPC.getStreamForReference(this.input[i]);					 
		}

        
		// create temporary file for storing results of merge 
        File tempFile = File.createTempFile("reduce_" + Integer.toString(nInputs) , ".tmp");
        OutputStream[] tempOutput = new OutputStream[1];
        tempOutput[0] = new FileOutputStream(tempFile);
        
        DataInputStream dis = null;
        
        // merge all input files into one sorted one
        SWTeraMerger merger = new SWTeraMerger();
        merger.merge(inputs, tempOutput, nInputs);
        try {
	        System.out.println("MapReduce: Reduce " + Integer.toString(id) + " merged file size: " 
	        		+ Long.toString(tempFile.length()) + " for job: " + jobID);      
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
        	System.out.println("MapReduce: Exception while running Reduce " + Integer.toString(id) 
        			+ " for job: " + jobID);
       	 	e.printStackTrace();
       } finally {
       		// close input streams
    		Utils.closeInputStream(dis);
    		for(int i = 0; i < nInputs; i++) {
    			Utils.closeInputStream(inputs[i]);
    		}
    		
        	// close output stream and delete temp file
    		tempFile.delete();
    		//Utils.closeOutputStream(tempOutput);
    		dos[0].flush();
    		Utils.closeOutputStream(dos[0]);	
        }
        
        Ciel.log("MapReduce: Reduce " + Integer.toString(id) + " finished in "
       		 + Double.toString((System.currentTimeMillis() - startTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);		
        System.out.println("MapReduce: Reduce " + Integer.toString(id) + " finished in "
		 + Double.toString((System.currentTimeMillis() - startTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);		
	}

	public void setup() {
		
	}
	
	public void run(DataOutputStream[] dos, DataInputStream dis) throws Exception {
		
	}

}
