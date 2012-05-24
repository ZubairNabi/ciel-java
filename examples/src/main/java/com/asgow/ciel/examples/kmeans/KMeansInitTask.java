package com.asgow.ciel.examples.kmeans;

import com.asgow.ciel.examples.mapreduce.common.DateTime;
import com.asgow.ciel.examples.mapreduce.common.Logger;
import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansInitTask implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		long startTime = System.currentTimeMillis();
		DateTime dateTime = new DateTime();
		int numVectors = Integer.parseInt(Ciel.args[0]);
		int numDimensions = Integer.parseInt(Ciel.args[1]);
		int k = Integer.parseInt(Ciel.args[2]);
		int numPartitions = Integer.parseInt(Ciel.args[3]);
		double epsilon = Double.parseDouble(Ciel.args[4]);
		boolean doCache = Boolean.parseBoolean(Ciel.args[5]);
		String jobID = "kmeans-" + Integer.toString(numPartitions) + "partitions";
		if(Ciel.args.length == 7) {
        	jobID = Ciel.args[6];
        }
		
		String taskID = "kmeans initTask";
		Logger logger = new Logger(jobID);
		logger.LogEvent(taskID, Logger.STARTED, 0);
		
		long dataGeneratorStartTime = System.currentTimeMillis();		
		//Reference randomData = Ciel.spawn(new KMeansDataGenerator(numVectors, numDimensions), null, 1)[0];
		Reference[] dataPartitions = Ciel.getRefsFromPackage("kmeans-vectors");
		if (dataPartitions == null) {
			dataPartitions = new Reference[numPartitions];
			for (int i = 0; i < numPartitions; ++i) {
				dataPartitions[i] = Ciel.spawn(new KMeansDataGenerator(numVectors, numDimensions, i, jobID, i), null, 1)[0];
			}
		}		
		Ciel.blockOn(dataPartitions);
		logger.LogEvent(taskID, "data generated", dataGeneratorStartTime);
		
		long initClustersStartTime = System.currentTimeMillis();
		Reference initClusters = Ciel.spawn(new KMeansHead(dataPartitions[0], k, numDimensions, jobID, 0), null, 1)[0];	
		Ciel.blockOn(initClusters);
		logger.LogEvent(taskID, "initClusters obtained", initClustersStartTime);
		
		long partialSumsStartTime = System.currentTimeMillis();
		Reference[] partialSumsRefs = new Reference[numPartitions];
		for (int i = 0; i < numPartitions; ++i) {
			partialSumsRefs[i] = Ciel.spawn(new KMeansMapper(dataPartitions[i], initClusters, k, numDimensions, doCache, jobID, i), null, 1)[0];
		}		
		Ciel.blockOn(partialSumsRefs);
		logger.LogEvent(taskID, "first wave of maps completed", partialSumsStartTime);
		
		Reference finalOutput = Ciel.spawn(new KMeansReducer(partialSumsRefs, initClusters, k, numDimensions, epsilon, dataPartitions, 0, doCache, jobID, 0), null, 1)[0];
		Ciel.blockOn(finalOutput);
		
		logger.LogEvent("kmeans initTask", Logger.FINISHED, initClustersStartTime);
		Ciel.returnPlainString("kmeans initTask completed! in "
				 + Double.toString((System.currentTimeMillis() - startTime)/1000.0) + " secs at " + dateTime.getCurrentDateTime() + " for job: " + jobID);
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

}
