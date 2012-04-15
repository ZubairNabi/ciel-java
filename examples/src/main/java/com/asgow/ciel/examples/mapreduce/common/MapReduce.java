package com.asgow.ciel.examples.mapreduce.common;

import java.io.IOException;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.examples.mapreduce.wordcount.WordCountMap;
import com.asgow.ciel.examples.mapreduce.wordcount.WordCountReduce;

public class MapReduce {
	
	public Reference[][] map(Reference mapInputs[], int numMaps, int numReduces) throws IOException {
		// create references for map task output
        Reference[][] mapResults = new Reference[numMaps][numReduces];
		
        // spawn map tasks
		for (int i = 0; i < numMaps; ++i) {
			mapResults[i] = Ciel.spawn(new WordCountMap(mapInputs[i], numReduces));
		}		
		return mapResults;
	}
	
	public Reference[][] shuffle(Reference[][] inputs, int numMaps, int numReduces) {
		Reference[][] outputs = new Reference[numReduces][numMaps];
		for(int i = 0; i < numReduces; ++i) {
			for (int j = 0; j < numMaps; ++j) {
				outputs[i][j] = inputs[j][i];
			}
		}
		return outputs;
	}
		
	public void reduce(Reference[][] reduceInput, int numReduces) throws IOException {
		// spawn reduce tasks
		for (int i = 0; i < numReduces; ++i) {
			Ciel.tailSpawn(new WordCountReduce(reduceInput[i]));
		}
	}
}
