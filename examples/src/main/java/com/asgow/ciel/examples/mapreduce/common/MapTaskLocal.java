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

public class MapTaskLocal implements ConstantNumOutputsTask {

    private String input;
    private int nReducers;
    private DateTime dateTime;
    private int id;
    private String jobID;
	
	public MapTaskLocal(String input, int nReducers, int id, String jobID) {
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
		File CIEL_TEMP_DIR = new File("/mnt/ciel_data/tmp/");
		long taskStartTime = System.currentTimeMillis();
		String taskID = "Map " + Integer.toString(id);
		//create logger
		Logger logger = new Logger(jobID);
		Ciel.log(taskID + " started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		logger.LogEvent(taskID, Logger.STARTED, 0);
        
        logger.LogEvent(taskID, Logger.ASSIGNED_INPUT + input, 0);
        
        // create a BufferedReader from input stream
        BufferedReader bufferedReader 
        = new BufferedReader(new InputStreamReader(new FileInputStream(this.input)));
        
        logger.LogEvent(taskID, Logger.FETCHED_INPUT, taskStartTime);
     
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
	        	tempFiles[i] = File.createTempFile("reduce_" + Integer.toString(i) , ".tmp", CIEL_TEMP_DIR);
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
	        logger.LogEvent(taskID, Logger.LOGIC_FINISHED, logicStartTime);
	       
	        long sortStartTime = System.currentTimeMillis();
			// now sort the temp files with 50 Mb mem limit
			for(int i = 0; i < nReducers; i++) {
				ssize += tempFiles[i].length();	
				tempIs[i] = new FileInputStream(tempFiles[i]);
				new SWTeraBucketer().invoke(tempIs[i], outputs[i], 1);
			}
			logger.LogEvent(taskID, Logger.OUTPUT_SIZE + Long.toString(ssize), 0);
	        logger.LogEvent(taskID, Logger.SORT_FINISHED, sortStartTime);
			
        } catch (Exception e) {
             logger.LogEvent(taskID, Logger.EXCEPTION, 0);
        	 e.printStackTrace();
        } finally {
    		// close output streams and delete temp files
        	for(int i = 0; i < nReducers; i++) {
        		logger.LogEvent(taskID, Logger.PRODUCED_OUTPUT 
		                + writableReferences[i].getFilename(), 0);
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
       		 + Double.toString((System.currentTimeMillis() - taskStartTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        logger.LogEvent(taskID, Logger.FINISHED, taskStartTime);
	}

	public void setup() {

	}
	
	public void run(BufferedReader bufferedReader, DataOutputStream[] dos, int numReducers) throws Exception {
		
	}

}
