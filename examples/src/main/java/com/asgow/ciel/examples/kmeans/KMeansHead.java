package com.asgow.ciel.examples.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;

import com.asgow.ciel.examples.mapreduce.common.Logger;
import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansHead implements FirstClassJavaTask {

	private final Reference dataPartitionRef;
	private final int k;
	private final int numDimensions;
	private String jobID;
	private int id;
	
	public KMeansHead(Reference dataPartitionRef, int k, int numDimensions, String jobID, int id) {
		this.dataPartitionRef = dataPartitionRef;
		this.k = k;
		this.numDimensions = numDimensions;
		this.jobID = jobID;
		this.id = id;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef };
	}

	@Override
	public void invoke() throws Exception {
		String taskID = "kmeansHead " + Integer.toString(id);
		long taskStartTime = System.currentTimeMillis();
		//create logger
		Logger logger = new Logger(jobID);
		logger.LogEvent(taskID, Logger.STARTED, 0);
		
		DataInputStream dis = new DataInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef)));
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		DataOutputStream dos = new DataOutputStream(out.open());
		for (int i = 0; i < this.k * this.numDimensions; ++i) {
			dos.writeDouble(dis.readDouble());
		}
		logger.LogEvent(taskID, Logger.FETCHED_INPUT, taskStartTime);
		dis.close();
		dos.close();
		logger.LogEvent(taskID,Logger.FINISHED, taskStartTime);
		//Ciel.RPC.closeOutput(0);
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

}
