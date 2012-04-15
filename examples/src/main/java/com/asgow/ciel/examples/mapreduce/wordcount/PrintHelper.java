package com.asgow.ciel.examples.mapreduce.wordcount;

import java.io.*;

import com.asgow.ciel.examples.mapreduce.common.Text;


public class PrintHelper {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
		    DataInputStream fis = new DataInputStream(new BufferedInputStream(new FileInputStream (args[0])));
			
			while (true) {
				Text word = new Text();
				IntWritable value = new IntWritable();
				try {
					word.readFields(fis);
					value.readFields(fis);
				} catch (EOFException e) {
					break;
				}

				System.out.println(word + " = " + value);
			}
		} catch (Exception e) {
			System.out.println("Oooooops... : ");
			e.printStackTrace();
		}


	}

}
