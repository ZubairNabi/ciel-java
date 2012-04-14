package skywriting.examples.wordcount;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.ConstantNumOutputsTask;

public class WordCountReduce implements ConstantNumOutputsTask {
        
    private Reference[] input;
	
	public WordCountReduce(Reference[] input) {
		this.input = input;
	}
	
	public Reference[] getDependencies() {
		return this.input;
	}
	
	public int getNumOutputs() {
		return 1;
	}

	public void invoke() throws Exception {
        System.out.println("WordCountReduce started");
        int nInputs = input.length;
		DataInputStream[] dis = new DataInputStream[nInputs];
		DataOutputStream[] dos = new DataOutputStream[1];
        OutputStream[] outputs = new OutputStream[1];
		
		for(int i = 0; i < nInputs; i++) {
			dis[i] = new DataInputStream(new BufferedInputStream(Ciel.RPC.getStreamForReference(this.input[i])));
		}
				
		WritableReference resultReference = Ciel.RPC.getOutputFilename(0);	
        outputs[0] = resultReference.open();
		dos[0] = new DataOutputStream(new BufferedOutputStream(outputs[0]));

		try {
			IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(dos, 1, Integer.MAX_VALUE, comb);
			
			for (int i = 0; i < dis.length; i++) {
				while (true) {
					Text word = new Text();
					IntWritable value = new IntWritable();
					try {
						word.readFields(dis[i]);
						value.readFields(dis[i]);
					} catch (EOFException e) {
						break;
					}

					System.out.println(word + " = " + value);
					outMap.collect(word, value);
				}
			}
			outMap.flushAll();
			for (DataOutputStream d : dos)
				d.close();
			
		} catch (IOException e) {
			System.out.println("IOException while running WordCountReduce");
			e.printStackTrace();
			System.exit(1);
		}

        System.out.println("WordCountReduce finished");
        int c;

		ByteArrayOutputStream horizontalChunkBuffer = new ByteArrayOutputStream();
        InputStream horizontalChunkInput = Ciel.RPC.getStreamForReference(resultReference.getCompletedRef());
		while ((c = horizontalChunkInput.read()) != -1) {
			horizontalChunkBuffer.write(c);
		}
		horizontalChunkInput.close();
		String horizontalChunkArray = new String(horizontalChunkBuffer.toByteArray());
		System.out.println(horizontalChunkArray);
		Ciel.returnPlainString("WordCountReduce finished and wrote to file " + resultReference.getFilename());
	}

	public void setup() {
		
	}

}
