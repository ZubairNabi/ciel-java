package com.asgow.ciel.examples.mapreduce.common;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class MapReduce {
	
    //TODO: Right now each input is required separately, change to get one input file with links to all other input files
	public Reference[] getReferencesFromPackage(int numInputs) {
		Reference[] references = new Reference[numInputs]; 
		for (int i = 0; i < numInputs; ++i) {
	        	references[i] = Ciel.RPC.packageLookup("input" + Integer.toString(i));
		}
		System.out.println("MapReduce: References obtained for " + Integer.toString(numInputs) + " inputs");
		return references;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Reference[][] map(String mapClassName, Reference mapInputs[], int numMaps, int numReduces) throws IOException, ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		// get map class object using reflection
		Class mapClass = Class.forName(mapClassName);
		Constructor mapConstructor = mapClass.getConstructor(new Class[] {
			Reference.class,
			int.class
		});
		Object[] parms = new Object[2];
		parms[1] = numReduces;
		
		// create references for map task output
        Reference[][] mapResults = new Reference[numMaps][numReduces];
		
        // spawn map tasks
		for (int i = 0; i < numMaps; ++i) {
			parms[0] = mapInputs[i];
			mapResults[i] = Ciel.spawn((ConstantNumOutputsTask) mapConstructor.newInstance(parms));
		}		
		System.out.println("MapReduce: " + Integer.toString(numMaps) + " map tasks spawned");
		return mapResults;
	}
	
	public Reference[][] shuffle(Reference[][] inputs, int numMaps, int numReduces) {
		Reference[][] outputs = new Reference[numReduces][numMaps];
		for(int i = 0; i < numReduces; ++i) {
			for (int j = 0; j < numMaps; ++j) {
				outputs[i][j] = inputs[j][i];
			}
		}
		System.out.println("MapReduce: Shuffle completed");
		return outputs;
	}
		
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void reduce(String reduceClassName, Reference[][] reduceInput, int numReduces) throws IOException, SecurityException, NoSuchMethodException, ClassNotFoundException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		// get reduce class object using reflection
		Class reduceClass = Class.forName(reduceClassName);
		Constructor reduceConstructor = reduceClass.getConstructor(new Class[] {
			Reference[].class
		});
		Object[] parms = new Object[1];
				
		// spawn reduce tasks
		for (int i = 0; i < numReduces; ++i) {
			parms[0] = reduceInput[i];
			Ciel.tailSpawn((FirstClassJavaTask) reduceConstructor.newInstance(parms));
		}
		System.out.println("MapReduce: " + Integer.toString(numReduces) + " reduce tasks spawned");
	}
	
	public Reference[] getReferencesFromInputFile(String inputFile) throws IOException {
		
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(inputFile))));
		Reference[] references = null;
		String line;
		
		while((line = bufferedReader.readLine())!= null) {
			;
		}
		return references;
	}
}
