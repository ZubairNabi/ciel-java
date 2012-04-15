package com.asgow.ciel.examples.mapreduce.sort;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.examples.mapreduce.common.MapReduce;

public class Sort implements FirstClassJavaTask {

	public void invoke() throws Exception {
		// check args
        if(Ciel.args.length != 2) {
        	Ciel.returnPlainString("Invalid number of arguments. Usage: com.asgow.ciel.examples.mapreduce.sort.Sort" +
        			" [NUM_OF_REDUCE_TASKS] [NUM_OF_INPUTS] -P input[n]=[PATH_TO_FILE], where 0 =< n < total_inputs");
        } 
        // get start time 
        long start = System.currentTimeMillis();
        
        // get number of reduce tasks and input files
        int numReduces = Integer.parseInt(Ciel.args[0]);
        int numInputs = Integer.parseInt(Ciel.args[1]);
              
        // create MapReduce object
        MapReduce mapReduce = new MapReduce();
    	
        // get input references
        Reference[] mapInputs = mapReduce.getReferencesFromPackage(numInputs);
        
        // create maps
        Reference[][] mapResults = mapReduce.map("com.asgow.ciel.examples.mapreduce.sort.SortMap", mapInputs, numInputs, numReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = mapReduce.shuffle(mapResults, numInputs, numReduces);
		
		mapReduce.reduce("com.asgow.ciel.examples.mapreduce.sort.SortReduce", reduceInput, numReduces);
		
		// get end time
		long end = System.currentTimeMillis();
		
		Ciel.blockOn(Ciel.jarLib);
		Ciel.returnPlainString("Job Completed in " + Long.toString(end - start) + "ms");			
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
}
