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
import java.net.InetAddress;

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
		String hostname = InetAddress.getLocalHost().getHostName();
		//File CIEL_TEMP_DIR = new File("/tmp/");
		File CIEL_TEMP_DIR = new File("/mnt/ssd/" + hostname + "/ciel_data/tmp/");
		//File CIEL_TEMP_DIR = new File("/mnt/ciel_data/tmp/");
		long taskStartTime = System.currentTimeMillis();
		String taskID = "Reduce " + Integer.toString(id);
		//create logger
		Logger logger = new Logger(jobID);
		Ciel.log("MapReduce: Reduce " + Integer.toString(id) + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        logger.LogEvent(taskID, Logger.STARTED, 0);
        
        // create input references
        int nInputs = input.length;
		DataOutputStream[] dos = new DataOutputStream[1];
        InputStream[] inputs = new InputStream[nInputs];
		for(int i = 0; i < nInputs; i++) {
			Ciel.blockOn(this.input[i]);
			inputs[i] = Ciel.RPC.getStreamForReference(this.input[i], 1024*1024*64, false, false, true);
			logger.LogEvent(taskID, Logger.ASSIGNED_INPUT
			        + this.input[i].toJson().get("__ref__").toString(), 0);				 
		}
		
		logger.LogEvent(taskID, Logger.FETCHED_INPUT, taskStartTime);
		
		// create temporary file for storing results of merge 
        File tempFile = File.createTempFile("reduce_" + Integer.toString(nInputs) , ".tmp", CIEL_TEMP_DIR);
        OutputStream[] tempOutput = new OutputStream[1];
        tempOutput[0] = new FileOutputStream(tempFile);
        
        DataInputStream dis = null;
        
        long mergeStartTime = System.currentTimeMillis();
        // merge all input files into one sorted one
        SWTeraMerger merger = new SWTeraMerger();
        merger.merge(inputs, tempOutput, nInputs);
        try {
        	logger.LogEvent(taskID, Logger.MERGE_SIZE 
	        		+ Long.toString(tempFile.length()), 0);
        	logger.LogEvent(taskID, Logger.MERGE_FINISHED, mergeStartTime);
	       
	        // create output file reference and get outputstream	
	        WritableReference writableReference = Ciel.RPC.getOutputFilename(0);
			dos[0] = new DataOutputStream(new BufferedOutputStream(writableReference.open()));

			// create input stream for the single sorted input file
	        dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
	        
	        long logicStartTime = System.currentTimeMillis();
	        //run reduce logic
	        run(dos, dis);
	        logger.LogEvent(taskID, Logger.LOGIC_FINISHED, logicStartTime);
        } catch (Exception e) {
        	logger.LogEvent(taskID, Logger.EXCEPTION, 0);
       	 	e.printStackTrace();
       } finally {
       		// close input streams
    		Utils.closeInputStream(dis);
    		for(int i = 0; i < nInputs; i++) {
    			Utils.closeInputStream(inputs[i]);
    		}
    		
        	// close output stream and delete temp file
    		tempFile.delete();
    		Utils.closeOutputStream(tempOutput[0]);
    		dos[0].flush();
    		Utils.closeOutputStream(dos[0]);	
        }
        
        Ciel.log("MapReduce: Reduce " + Integer.toString(id) + " finished in "
       		 + Double.toString((System.currentTimeMillis() - taskStartTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);	
        logger.LogEvent(taskID,Logger.FINISHED, taskStartTime);
	}

	public void setup() {
		
	}
	
	public void run(DataOutputStream[] dos, DataInputStream dis) throws Exception {
		
	}

}
