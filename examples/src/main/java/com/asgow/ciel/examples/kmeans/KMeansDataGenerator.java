package com.asgow.ciel.examples.kmeans;

import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;

import com.asgow.ciel.examples.mapreduce.common.Logger;
import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansDataGenerator implements FirstClassJavaTask {

	private int numVectors;
	private int numDimensions;
	private int seed;
	private String jobID;
	private int id;
	
	public KMeansDataGenerator(int numVectors, int numDimensions, int seed, String jobID, int id) {
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.seed = seed;
		this.jobID = jobID;
		this.id = id;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		String taskID = "kmeansDataGenerator " + Integer.toString(id);
		long taskStartTime = System.currentTimeMillis();
		//create logger
		Logger logger = new Logger(jobID);
		logger.LogEvent(taskID, Logger.STARTED, 0);
		double minValue = -1000000.0;
		double maxValue = 1000000.0;
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(out.open(), 1048576));
		
		Random rand = new Random(this.seed);
		
		for (int i = 0; i < this.numVectors; ++i) {
			for (int j = 0; j < this.numDimensions; ++j) {
				dos.writeDouble((rand.nextDouble() * (maxValue - minValue)) + minValue);
			}
		}
		logger.LogEvent(taskID, Logger.OUTPUT_SIZE + Long.toString(8 * this.numVectors * this.numDimensions), 0);
		dos.close();
		logger.LogEvent(taskID, Logger.FINISHED, taskStartTime);
		//Ciel.RPC.closeOutput(0);
	}

	@Override
	public void setup() {
		;
	}

	public static void main(String[] args) {
		try {
			int numVectors = Integer.parseInt(args[0]);
			int numDimensions = Integer.parseInt(args[1]);
			int seed = Integer.parseInt(args[2]);
			String jobID = args[3];
			int id = Integer.parseInt(args[3]);
			Ciel.RPC = new DummyRPC(new OutputStream[] { new FileOutputStream(args[3]) });
			new KMeansDataGenerator(numVectors, numDimensions, seed, jobID, id).invoke();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
}
