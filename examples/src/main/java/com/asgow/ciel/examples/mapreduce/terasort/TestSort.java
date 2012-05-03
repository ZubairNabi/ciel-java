package com.asgow.ciel.examples.mapreduce.terasort;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;


public class TestSort implements FirstClassJavaTask {

	public void setup() {
		// TODO Auto-generated method stub
		
	}

	public void invoke() throws Exception {
		System.out.println("SortTest started");
		File outputFile = File.createTempFile("outputFile" , ".tmp");
		Reference inputReference = Ciel.RPC.packageLookup("input");
		InputStream[] in = new InputStream[1];
		OutputStream[] out = new OutputStream[1];
		try {
			in[0] = Ciel.RPC.getStreamForReference(inputReference);
			out[0] = new FileOutputStream(outputFile);
			new SWTeraBucketer().invoke(in, out, 1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		InputStream inTemp = new FileInputStream(outputFile);
		DataInputStream indata = new DataInputStream(inTemp);
		  BufferedReader br = new BufferedReader(new InputStreamReader(indata));
		  String strLine;
		  //Read File Line By Line
		  while ((strLine = br.readLine()) != null)   {
		  // Print the content on the console
			  System.out.println (strLine);
		  }
		indata.close();
		in[0].close();
		out[0].close();
		outputFile.delete();
		
		Ciel.returnPlainString("SortTest completed");
		
	}

	public Reference[] getDependencies() {
		// TODO Auto-generated method stub
		return null;
	}
	    
}
