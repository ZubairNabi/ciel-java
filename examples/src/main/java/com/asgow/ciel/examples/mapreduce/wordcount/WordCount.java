package com.asgow.ciel.examples.mapreduce.wordcount;

import java.io.IOException;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.examples.mapreduce.common.MapReduce;

public class WordCount implements FirstClassJavaTask {

	public void invoke() throws Exception {
		// check args
        if(Ciel.args.length != 2) {
        	Ciel.returnPlainString("Invalid number of arguments");
        } 
        // get number of map and reduce tasks
        int numMaps = Integer.parseInt(Ciel.args[0]);
        int numReduces = Integer.parseInt(Ciel.args[1]);
    	
        // get input references
        Reference[] mapInputs = new Reference[numMaps]; 
        //TODO: Right now all maps are assigned the same input file, change to assign each a different one
        for (int i = 0; i < numMaps; ++i) {
        	mapInputs[i] = Ciel.RPC.packageLookup("input" + Integer.toString(i));
		}
        
        // create MapReduce object
        MapReduce mapReduce = new MapReduce();
        
        // create maps
        Reference[][] mapResults = mapReduce.map("com.asgow.ciel.examples.mapreduce.wordcount.WordCountMap", mapInputs, numMaps, numReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = mapReduce.shuffle(mapResults, numMaps, numReduces);
		
		mapReduce.reduce("com.asgow.ciel.examples.mapreduce.wordcount.WordCountReduce", reduceInput, numReduces);
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
}
