package com.asgow.ciel.examples.mapreduce.common;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import com.asgow.ciel.examples.mapreduce.common.DateTime;
import com.asgow.ciel.examples.mapreduce.common.Utils;
import com.asgow.ciel.examples.mapreduce.terasort.SWTeraBucketer;
import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;
import com.google.gson.JsonParser;

public class MapTask implements ConstantNumOutputsTask {

    private String input;
    private int nReducers;
    private DateTime dateTime;
    private int id;
    private String jobID;
	
	public MapTask(String input, int nReducers, int id, String jobID) {
		this.input = input;
		this.nReducers = nReducers;
		dateTime = new DateTime();
		this.id = id;
		this.jobID = jobID;
	}
	
	public int getNumOutputs() {
		return this.nReducers;
	}
	
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	public void invoke() throws Exception {
		long taskStartTime = System.currentTimeMillis();
		Ciel.log("MapReduce: Map " + Integer.toString(id) + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        System.out.println("MapReduce: Map " + Integer.toString(id) + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        
        //create input reference
        Reference indexFileRef = Reference.fromJson(new JsonParser().parse(input).getAsJsonObject());
        System.out.println("MapReduce: Map " + Integer.toString(id) + " assigned input reference: " 
        + (new JsonParser().parse(input).getAsJsonObject().get("__ref__").toString()) + " at " 
        + dateTime.getCurrentDateTime() + " for job: " + jobID);
        
        // create a BufferedReader from input stream
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Ciel.RPC.getStreamForReference(indexFileRef)));
        
        System.out.println("MapReduce: Map " + Integer.toString(id) + " input fetch completed in "
				 + Double.toString((System.currentTimeMillis() - taskStartTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        
        // number of output files would be equal to number of reducers, so creating that many outputstreams and references
        WritableReference[] writableReferences = new WritableReference[nReducers];
        OutputStream[] outputs = new OutputStream[nReducers];
        
        // create temp files to store unsorted results
        File tempFiles[] = new File[nReducers];
        DataOutputStream[] tempDos = new DataOutputStream[nReducers];
        InputStream[] tempIs = new InputStream[nReducers];
        
        try {
	        for(int i = 0; i < nReducers; i++) {
	        	// create temp files and output streams
	        	tempFiles[i] = File.createTempFile("reduce_" + Integer.toString(i) , ".tmp");
				tempDos[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFiles[i])));
				
				// get references for output files and convert to OutputStream
				writableReferences[i] = Ciel.RPC.getOutputFilename(i);
	        	outputs[i] = writableReferences[i].open();
	        	//tempDos[i] = new DataOutputStream(new BufferedOutputStream(outputs[i]));
			}
	        long ssize = 0;
	        long logicStartTime = System.currentTimeMillis();
	        // call map logic
	        run(bufferedReader, tempDos, nReducers);
	        System.out.println("MapReduce: Map " + Integer.toString(id) + " logic completed in "
					 + Double.toString((System.currentTimeMillis() - logicStartTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
	        long sortStartTime = System.currentTimeMillis();
			// now sort the temp files with 50 Mb mem limit
			for(int i = 0; i < nReducers; i++) {
				ssize += tempFiles[i].length();	
				tempIs[i] = new FileInputStream(tempFiles[i]);
				new SWTeraBucketer().invoke(tempIs[i], outputs[i], 1);
			}
			System.out.println("MapReduce: Map " + Integer.toString(id) + " total size of map output files: " + Long.toString(ssize) + " for job: " + jobID);
			System.out.println("MapReduce: Map " + Integer.toString(id) + " sort completed in "
					 + Double.toString((System.currentTimeMillis() - sortStartTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        } catch (Exception e) {
        	 System.out.println("MapReduce: Exception while running Map " + Integer.toString(id)
        			 + " for job: " + jobID);
        	 e.printStackTrace();
        } finally {
    		// close output streams and delete temp files
        	for(int i = 0; i < nReducers; i++) {
        		System.out.println("MapReduce: Map " + Integer.toString(id) + " produced output reference: " 
		                + writableReferences[i].getFilename() + " at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		        tempDos[i].flush();
		        tempFiles[i].delete();
		        Utils.closeOutputStream(tempDos[i]);
		        outputs[i].flush();
		        Utils.closeOutputStream(outputs[i]);
		        Utils.closeInputStream(tempIs[i]);
			}
        	// close input stream
    		bufferedReader.close();
        }
        
        Ciel.log("MapReduce: Map " + Integer.toString(id) + " finished in "
       		 + Double.toString((System.currentTimeMillis() - taskStartTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        System.out.println("MapReduce: Map " + Integer.toString(id) + " finished in "
		 + Double.toString((System.currentTimeMillis() - taskStartTime)/1000) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
	}

	public void setup() {

	}
	
	public void run(BufferedReader bufferedReader, DataOutputStream[] dos, int numReducers) throws Exception {
		
	}

}
