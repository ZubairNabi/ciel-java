package com.asgow.ciel.examples.mapreduce.sort;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.examples.mapreduce.common.DateTime;
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
        System.out.println("Sort job started at " + dateTime.getCurrentDateTime());
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
         
        // create MapReduce object
        MapReduce mapReduce = new MapReduce(jobID);
    	
        // get input jsonelement strings for references
        String[] mapInputs = mapReduce.getReferencesFromInputFile(indexFile,
        		numInputs, indexFileSize, hostnames, ports, numReplicas);
        
        // check that getReferencesFromInputFile did not return a null
        if(mapInputs == null) {
    		Ciel.returnPlainString("MapReduce: Error! NUM_OF_INPUTS exceeded number of references in reference index");
    		System.exit(1);
        }
        
        // create maps
        Reference[][] mapResults = mapReduce.map("com.asgow.ciel.examples.mapreduce.sort.SortMap", mapInputs, numInputs, numReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = mapReduce.shuffle(mapResults, numInputs, numReduces);
		
		Reference[] reduceResults = mapReduce.reduce("com.asgow.ciel.examples.mapreduce.sort.SortReduce", reduceInput, numReduces);
		
		Ciel.blockOn(reduceResults);
		System.out.println("MapReduce: Sort job completed at " + dateTime.getCurrentDateTime());
		Ciel.returnPlainString("MapReduce: Sort completed! in "
		 + Long.toString((System.currentTimeMillis() - startTime)/1000) + " secs at "+ dateTime.getCurrentDateTime());
		
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
}
