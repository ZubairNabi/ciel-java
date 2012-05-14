package com.asgow.ciel.examples.mapreduce.sort;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.examples.mapreduce.common.DateTime;
import com.asgow.ciel.examples.mapreduce.common.Logger;
import com.asgow.ciel.examples.mapreduce.common.MapReduce;

public class Sort implements FirstClassJavaTask {

	public void invoke() throws Exception {
		long startTime = System.currentTimeMillis();
		// check args
        if(Ciel.args.length < 7) {
        	Ciel.returnPlainString("MapReduce: Invalid number of arguments. Usage: com.asgow.ciel.examples.mapreduce.sort.Sort" +
        			" [NUM_OF_INPUTS] [NUM_OF_REDUCE_TASKS] [REF_INDEX_FILENAME] [REF_INDEX_FILE_SIZE]" +
        			" [NUM_REPLICAS] [HOSTNAME_FOR_EACH_REPLICA] [PORT_FOR_EACH_REPLICA] [OPTIONAL_JOB_ID]");
        } 
        DateTime dateTime = new DateTime();
        // parse input args
        int numInputs = Integer.parseInt(Ciel.args[0]);
        int numReduces = Integer.parseInt(Ciel.args[1]);
        String indexFile = Ciel.args[2];
        int indexFileSize = Integer.parseInt(Ciel.args[3]);
        int numReplicas = Integer.parseInt(Ciel.args[4]);
        String[] hostnames  = new String[numReplicas];
        short[] ports  = new short[numReplicas];
        String jobID = "sort-" + Integer.toString(numInputs) + "maps-" + Integer.toString(numReduces) + "reduces";
        
        for(int i = 0; i < numReplicas; ++i) {
        	hostnames[i] = Ciel.args[i + 5];
        	ports[i] = Short.parseShort(Ciel.args[i + 5 + numReplicas]);
        }
        
        if(Ciel.args.length == (numReplicas * 2 + 5 + 1)) {
        	jobID = Ciel.args[numReplicas * 2 + 5];
        }
        //Get logger object
        Logger logger = new Logger(jobID);
        Ciel.log("MapReduce: Sort job started at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
        logger.LogEvent("Sort job", Logger.STARTED, 0);
        
        // create MapReduce object
        MapReduce mapReduce = new MapReduce(jobID);
    	// run MapReduce
        mapReduce.run(indexFile, numInputs, indexFileSize, hostnames, ports, numReplicas, numReduces,
        		"com.asgow.ciel.examples.mapreduce.sort.SortMap",
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
