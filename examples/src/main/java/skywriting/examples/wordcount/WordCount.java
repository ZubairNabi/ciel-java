package skywriting.examples.wordcount;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class WordCount implements FirstClassJavaTask {

	public void invoke() throws Exception {
		// check args
        if(Ciel.args.length != 2) {
        	Ciel.returnPlainString("Invalid number of arguments");
        } 
        // get number of map and reduce tasks
        int numMaps = Integer.parseInt(Ciel.args[0]);
        int numReduces = Integer.parseInt(Ciel.args[1]);
    	
        // get input references
        Reference[] mapInputs = new Reference[numMaps]; 
        //TODO: Right now all maps are assigned the same input file, change to assign each a different one
        for (int i = 0; i < numMaps; ++i) {
        	mapInputs[i] = Ciel.RPC.packageLookup("input" + Integer.toString(i));
		}

        // create references for map task output
        Reference[][] mapResults = new Reference[numMaps][numReduces];
		
        // spawn map tasks
		for (int i = 0; i < numMaps; ++i) {
			mapResults[i] = Ciel.spawn(new WordCountMap(mapInputs[i], numReduces));
		}
		
		// now shuffle map outputs so that each reduce task receives an input file from each map
		Reference[][] reduceInput = shuffle(mapResults, numMaps, numReduces);
		
		// spawn reduce tasks
		for (int i = 0; i < numReduces; ++i) {
			Ciel.tailSpawn(new WordCountReduce(reduceInput[i]));
		}
	}

	public void setup() {

	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	public Reference[][] shuffle(Reference[][] inputs, int numMaps, int numReduces) {
		Reference[][] outputs = new Reference[numReduces][numMaps];
		for(int i = 0; i < numReduces; ++i) {
			for (int j = 0; j < numMaps; ++j) {
				outputs[i][j] = inputs[j][i];
			}
		}
		return outputs;
	}

}
