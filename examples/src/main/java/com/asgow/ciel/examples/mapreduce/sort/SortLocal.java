package com.asgow.ciel.examples.mapreduce.sort;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.examples.mapreduce.common.DateTime;
import com.asgow.ciel.examples.mapreduce.common.Logger;
import com.asgow.ciel.examples.mapreduce.common.MapReduce;

public class SortLocal implements FirstClassJavaTask {

	public void invoke() throws Exception {
		long startTime = System.currentTimeMillis();
		// check args
        if(Ciel.args.length < 3) {
        	Ciel.returnPlainString("MapReduce: Invalid number of arguments. Usage: com.asgow.ciel.examples.mapreduce.sort.Sort" +
        			" [NUM_OF_INPUTS] [NUM_OF_REDUCE_TASKS] [PATH_TO_INPUT_FOLDER] [OPTIONAL_JOB_ID]");
        } 
        DateTime dateTime = new DateTime();
        // parse input args
        int numInputs = Integer.parseInt(Ciel.args[0]);
        int numReduces = Integer.parseInt(Ciel.args[1]);
        String indexFile = Ciel.args[2];
        
        String jobID = "sort-" + Integer.toString(numInputs) + "maps-" + Integer.toString(numReduces) + "reduces";
        
        
        if(Ciel.args.length == 4) {
        	jobID = Ciel.args[3];
        }
        //Get logger object
        Logger logger = new Logger(jobID);
        Ciel.log("MapReduce: Sort job started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        logger.LogEvent("Sort job", Logger.STARTED, 0);
        
        // create MapReduce object
        MapReduce mapReduce = new MapReduce(jobID);
    	// run MapReduce
        mapReduce.run(indexFile, numInputs, numReduces,
        		"com.asgow.ciel.examples.mapreduce.sort.SortMapLocal",
        		"com.asgow.ciel.examples.mapreduce.sort.SortReduce");
        
        Ciel.log("MapReduce: Sort completed! in "
				 + Double.toString((System.currentTimeMillis() - startTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		logger.LogEvent("Sort job", Logger.FINISHED, startTime);
		Ciel.returnPlainString("MapReduce: Sort completed! in "
		 + Double.toString((System.currentTimeMillis() - startTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
}
