package com.asgow.ciel.examples.mapreduce.sort;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import com.asgow.ciel.examples.mapreduce.wordcount.Text;

public class InputTest {
	 public static void main(String[] args) {
		 File inputFile = new File("/root/2M600.in");
		 File outFile = new File("/root/output.out");
		 BufferedReader bufferedReader = null; 
		 try {
			 outFile.createNewFile();
			bufferedReader 
			 = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
			DataOutputStream dos =
			new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
			int nReducers = 10;
			File tempFiles[] = new File[nReducers];
	        DataOutputStream[] tempDos = new DataOutputStream[nReducers];
			 for(int i = 0; i < nReducers; i++) {
		        	// create temp files and output streams
		        	tempFiles[i] = File.createTempFile("reduce_" + Integer.toString(i) , ".tmp");
					tempDos[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFiles[i])));
			 }
			
			String line;
			while ((line = bufferedReader.readLine()) != null) { 
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					Text word = new Text();
			        Text blank = new Text("");
					dos.writeChars(itr.nextToken());
				}

			}
			 for(int i = 0; i < nReducers; i++) {
				 tempDos[i].close();
				 tempFiles[i].delete();
			 }
			bufferedReader.close();
			dos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
}