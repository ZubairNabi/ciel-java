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
        logger.LogEvent("Sort job started");
        // create MapReduce object
        MapReduce mapReduce = new MapReduce(jobID);
    	
        // get input jsonelement strings for references
        String[] mapInputs = mapReduce.getFilesFromFolder(indexFile, numInputs);
        for(int i = 0; i < numInputs; ++i) {
        	System.out.println(mapInputs[i]);
        }
        /*
        // check that getReferencesFromInputFile did not return a null
        if(mapInputs == null) {
    		Ciel.returnPlainString("MapReduce: Error! NUM_OF_INPUTS exceeded number of references in reference index" + " for job: " + jobID);
    		System.exit(1);
        }
        
        // create maps
        Reference[][] mapResults = mapReduce.map("com.asgow.ciel.examples.mapreduce.sort.SortMap", mapInputs, numInputs, numReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = mapReduce.shuffle(mapResults, numInputs, numReduces);
		
		Reference[] reduceResults = mapReduce.reduce("com.asgow.ciel.examples.mapreduce.sort.SortReduce", reduceInput, numReduces);
		
		Ciel.blockOn(reduceResults);
		Ciel.log("MapReduce: Sort completed! in "
				 + Double.toString((System.currentTimeMillis() - startTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		logger.LogEventTimestamp("Sort completed!", startTime);
		*/
		Ciel.returnPlainString("MapReduce: Sort completed! in "
		 + Double.toString((System.currentTimeMillis() - startTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
}
