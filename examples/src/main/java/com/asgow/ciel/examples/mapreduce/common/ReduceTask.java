package com.asgow.ciel.examples.mapreduce.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.asgow.ciel.examples.mapreduce.common.MergeFiles;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class ReduceTask implements ConstantNumOutputsTask {
        
    private Reference[] input;
	
	public ReduceTask(Reference[] input) {
		this.input = input;
	}
	
	public Reference[] getDependencies() {
		return this.input;
	}
	
	public int getNumOutputs() {
		return 1;
	}

	public void invoke() throws Exception {
        System.out.println("Reduce started at " + System.currentTimeMillis());
        int nInputs = input.length;
		InputStream[] is = new InputStream[nInputs];
		DataOutputStream[] dos = new DataOutputStream[1];
        OutputStream[] outputs = new OutputStream[1];
        List<InputStream> listStreams = new ArrayList<InputStream>();
		
		for(int i = 0; i < nInputs; i++) {
			is[i] = new BufferedInputStream(Ciel.RPC.getStreamForReference(this.input[i]));
			listStreams.add(i, is[i]);
		}
        
		// create temporary file for storing results of merge 
        File tempFile = File.createTempFile("reduce_" + Integer.toString(nInputs) , ".tmp");
        FileOutputStream tempOutput = new FileOutputStream(tempFile);
        
        // merge all input files into one sorted one
        MergeFiles mergeFiles = new MergeFiles();
        mergeFiles.mergeFiles(listStreams, tempOutput);
		
        // create output file reference and get outputstream
		WritableReference resultReference = Ciel.RPC.getOutputFilename(0);	
        outputs[0] = resultReference.open();
		dos[0] = new DataOutputStream(new BufferedOutputStream(outputs[0]));
		
		// create input stream for the single sorted input file
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
        
        //run reduce logic
        run(dos, dis);
        
        // close output stream and delete temp file
		dos[0].close();			
		tempFile.delete();

        System.out.println("Reduce finished at " + System.currentTimeMillis());		
	}

	public void setup() {
		
	}
	
	public void run(DataOutputStream[] dos, DataInputStream dis) {
		
	}

}
