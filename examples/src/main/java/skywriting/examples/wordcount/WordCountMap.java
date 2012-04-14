package skywriting.examples.wordcount;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.StringTokenizer;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class WordCountMap implements ConstantNumOutputsTask {

    private Reference input;
    private int nReducers;
    private final static IntWritable one = new IntWritable(1);
	
	public WordCountMap(Reference input, int nReducers) {
		this.input = input;
		this.nReducers = nReducers;
	}
	
	public int getNumOutputs() {
		return this.nReducers;
	}
	
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	public void invoke() throws Exception {
        System.out.println("WordCountMap started");
        
        // convert input reference to stream and then create a BufferedReader
        InputStream inputStream = Ciel.RPC.getStreamForReference(this.input);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        
        // number of output files would be equal to number of reducers, so creating that many outputstreams and references
        OutputStream[] outputs = new OutputStream[nReducers];
        DataOutputStream[] dos = new DataOutputStream[nReducers];
        WritableReference[] resultReference = new WritableReference[nReducers];

        // get references for output files and covert to DataOutputStream
        for(int i = 0; i < nReducers; i++) {
        	resultReference[i] = Ciel.RPC.getOutputFilename(i);
        	outputs[i] = resultReference[i].open();
			dos[i] = new DataOutputStream(new BufferedOutputStream(outputs[i]));
		}

        String line;
        try {
        	IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(dos, nReducers, 1000, comb);
			while ((line = bufferedReader.readLine()) != null) { 
				//System.out.println(line);
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					Text word = new Text();
					word.set(itr.nextToken());
					outMap.collect(word, one);
				}
			}
			outMap.flushAll();
			for (DataOutputStream d : dos) 
				d.close();
		} catch (IOException e) {
			System.out.println("IOException while running WordCountMap");
			e.printStackTrace();
			System.exit(1);
		}

        System.out.println("WordCountMap finished");
	}

	public void setup() {

	}

}
