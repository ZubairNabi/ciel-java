package com.asgow.ciel.examples.mapreduce.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.ConcreteReference;
import com.asgow.ciel.references.Netloc;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class MapReduce {
	
	private DateTime dateTime;
	private String jobID;
	
	public MapReduce(String jobID) {
		this.jobID = jobID;
		dateTime = new DateTime();
	}
	
    //TODO: Right now each input is required separately, change to get one input file with links to all other input files
	public Reference[] getReferencesFromPackage(int numInputs) {
		Reference[] references = new Reference[numInputs]; 
		for (int i = 0; i < numInputs; ++i) {
	        	references[i] = Ciel.RPC.packageLookup("input" + Integer.toString(i));
		}
		System.out.println("MapReduce: References obtained for " + Integer.toString(numInputs) + " inputs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		return references;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Reference[][] map(String mapClassName, String mapInputs[], int numMaps, int numReduces) throws IOException, ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		// get map class object using reflection
		Class mapClass = Class.forName(mapClassName);
		Constructor mapConstructor = mapClass.getConstructor(new Class[] {
			String.class,
			int.class,
			int.class,
			String.class
		});
		Object[] parms = new Object[4];
		parms[1] = numReduces;
		parms[3] = jobID;
		
		// create references for map task output
        Reference[][] mapResults = new Reference[numMaps][numReduces];
		
        // spawn map tasks
		for (int i = 0; i < numMaps; ++i) {
			parms[0] = mapInputs[i];
			parms[2] = i;
			mapResults[i] = Ciel.spawn((ConstantNumOutputsTask) mapConstructor.newInstance(parms));
		}		
		System.out.println("MapReduce: " + Integer.toString(numMaps) + " map tasks spawned at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		return mapResults;
	}
	
	public Reference[][] shuffle(Reference[][] inputs, int numMaps, int numReduces) {
		Reference[][] outputs = new Reference[numReduces][numMaps];
		for(int i = 0; i < numReduces; ++i) {
			for (int j = 0; j < numMaps; ++j) {
				outputs[i][j] = inputs[j][i];
			}
		}
		System.out.println("MapReduce: Shuffle completed at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		return outputs;
	}
		
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Reference[] reduce(String reduceClassName, Reference[][] reduceInput, int numReduces) throws IOException, SecurityException, NoSuchMethodException, ClassNotFoundException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		// get reduce class object using reflection
		Class reduceClass = Class.forName(reduceClassName);
		Constructor reduceConstructor = reduceClass.getConstructor(new Class[] {
			Reference[].class,
			int.class,
			String.class
		});
		Object[] parms = new Object[3];
		parms[2] = jobID;
		
		// create references for reduce task output
        Reference[] reduceResults = new Reference[numReduces];
				
		// spawn reduce tasks
		for (int i = 0; i < numReduces; ++i) {
			parms[0] = reduceInput[i];
			parms[1] = i;
			reduceResults[i] = Ciel.spawn((ConstantNumOutputsTask) reduceConstructor.newInstance(parms))[0];
		}
		System.out.println("MapReduce: " + Integer.toString(numReduces) + " reduce tasks spawned at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		return reduceResults;
	}
	
	public String[] getReferencesFromInputFile(String inputFile, int nInputs,
			int inputFileSize, String hostnames[], short ports[], int nReplicas) throws IOException {
		// create new concrete reference for index file
		ConcreteReference indexFileRef = new ConcreteReference(inputFile, inputFileSize);
		// add locations for each replica
		for(int i = 0; i < nReplicas; ++i) {
			indexFileRef.addLocation(new Netloc(hostnames[i], ports[i]));
		}
		// number of output json elements is equal to the number of inputs
		String[] inputJsonElements = new String[nInputs];
		// get reader from reference stream
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Ciel.RPC.getStreamForReference(indexFileRef)));
		String line;
		StringBuffer stringBuffer = new StringBuffer();
		// read the contents of the reference
		while ((line = bufferedReader.readLine()) != null) {
			stringBuffer.append(line);
		}
		// parse string to get entire contents as json element
		JsonElement jsonElement = new JsonParser().parse(stringBuffer.toString());
		// convert json element to an array
		JsonArray jsonArray= jsonElement.getAsJsonArray();
		// check that specified number of input files is not more than the references in index file
		if(nInputs > jsonArray.size()) {
			return null;
		}
		for (int i = 0; i < nInputs; ++i) {
			inputJsonElements[i] = jsonArray.get(i).toString();
		}	
		System.out.println("MapReduce: JsonElements obtained for " + Integer.toString(nInputs) + " inputs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
		return inputJsonElements;
	}
	
	public String[] getFilesFromFolder(String folderPath, int nInputs) {
		String[] filePaths = new String[nInputs];
		File folderFile = new File(folderPath);
		File[] files = folderFile.listFiles();
		for(int i = 0; i < nInputs; ++i) {
			filePaths[i] = files[i].getAbsolutePath();
		}
		return filePaths;
	}
	
	public void run(String indexFile, int nInputs,
			int indexFileSize, String hostnames[], short ports[], int nReplicas, int nReduces,
			String mapTask, String reduceTask) throws Exception {
	     // get input jsonelement strings for references
        String[] mapInputs = getReferencesFromInputFile(indexFile,
        		nInputs, indexFileSize, hostnames, ports, nReplicas);
        
        // check that getReferencesFromInputFile did not return a null
        if(mapInputs == null) {
    		Ciel.returnPlainString("MapReduce: Error! NUM_OF_INPUTS exceeded number of references in reference index" + " for job: " + jobID);
    		System.exit(1);
        }
        
        // create maps
        Reference[][] mapResults = map(mapTask, mapInputs, nInputs, nReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = shuffle(mapResults, nInputs, nReduces);
		
		// create reduces
		Reference[] reduceResults = reduce(reduceTask, reduceInput, nReduces);
		
		// wait for reduce outputs
		Ciel.blockOn(reduceResults);
	}
	
	public void run(String indexFile, int nInputs, int nReduces,
			String mapTask, String reduceTask) throws Exception {
		 
		// get list of input files 
	    String[] mapInputs = getFilesFromFolder(indexFile, nInputs);

        // check that getReferencesFromInputFile did not return a null
        if(mapInputs == null) {
    		Ciel.returnPlainString("MapReduce: Error! NUM_OF_INPUTS exceeded number of references in reference index" + " for job: " + jobID);
    		System.exit(1);
        }
        
        // create maps
        Reference[][] mapResults = map(mapTask, mapInputs, nInputs, nReduces);
        
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = shuffle(mapResults, nInputs, nReduces);
		
		// create reduces
		Reference[] reduceResults = reduce(reduceTask, reduceInput, nReduces);
		
		// wait for reduce outputs
		Ciel.blockOn(reduceResults);
	}
}
