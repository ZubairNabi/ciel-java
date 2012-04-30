package com.asgow.ciel.examples.mapreduce.wordcount;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.examples.mapreduce.common.MapReduce;
import com.google.gson.JsonElement;

public class WordCount implements FirstClassJavaTask {

	public void invoke() throws Exception {
		// check args
        if(Ciel.args.length < 7) {
        	Ciel.returnPlainString("Invalid number of arguments. Usage: com.asgow.ciel.examples.mapreduce.wordcount.WordCount" +
        			" [NUM_OF_INPUTS] [NUM_OF_REDUCE_TASKS] [REF_INDEX_FILENAME] [REF_INDEX_FILE_SIZE]" +
        			" [NUM_REPLICAS] [HOSTNAME_FOR_EACH_REPLICA] [PORT_FOR_EACH_REPLICA]");
        } 
        
        // parse input args
        int numInputs = Integer.parseInt(Ciel.args[0]);
        int numReduces = Integer.parseInt(Ciel.args[1]);
        String indexFile = Ciel.args[2];
        int indexFileSize = Integer.parseInt(Ciel.args[3]);
        int numReplicas = Integer.parseInt(Ciel.args[4]);
        String[] hostnames  = new String[numReplicas];
        short[] ports  = new short[numReplicas];
        
        for(int i = 0; i < numReplicas; ++i) {
        	hostnames[i] = Ciel.args[i + 5];
        	ports[i] = Short.parseShort(Ciel.args[i + 5 + numReplicas]);
        }
         
        // create MapReduce object
        MapReduce mapReduce = new MapReduce();
    	
        // get input jsonelement strings for references
        String[] mapInputs = mapReduce.getReferencesFromInputFile(indexFile,
        		numInputs, indexFileSize, hostnames, ports, numReplicas);
        
        // check that getReferencesFromInputFile did not return a null
        if(mapInputs == null) {
    		Ciel.returnPlainString("Error! NUM_OF_INPUTS exceeded number of references in reference index");
    		System.exit(1);
        }
        
        // create maps
        Reference[][] mapResults = mapReduce.map("com.asgow.ciel.examples.mapreduce.wordcount.WordCountMap", mapInputs, numInputs, numReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = mapReduce.shuffle(mapResults, numInputs, numReduces);
		
		Reference[] reduceResults = mapReduce.reduce("com.asgow.ciel.examples.mapreduce.wordcount.WordCountReduce", reduceInput, numReduces);
		
		Ciel.blockOn(reduceResults);
		Ciel.returnPlainString("WordCount completed!");
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
}
