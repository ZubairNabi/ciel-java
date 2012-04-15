package skywriting.examples.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriter;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.Merger;
import com.fasterxml.sort.std.ByteArrayComparator;
import com.fasterxml.sort.std.RawTextLineReader;
import com.fasterxml.sort.std.RawTextLineWriter;

public class MergeFiles {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> void merge(List<File> inputs, DataWriter<T> writer)
	        throws IOException {
	     ArrayList<DataReader<T>> readers = new ArrayList<DataReader<T>>(inputs.size());
	   	 DataReaderFactory readerFactory;
	     Comparator comparator;
	     readerFactory = RawTextLineReader.factory();
         comparator = new ByteArrayComparator();
	        try {
	            for (File mergedInput : inputs) {
	                readers.add(readerFactory.constructReader(new FileInputStream(mergedInput)));
	            }
	            DataReader<T> merger = Merger.mergedReader(comparator, readers);
	            T value;
	            while ((value = merger.readNext()) != null) {
	                writer.writeEntry(value);
	            }
	            writer.close();
	        } finally {
	            for (File input : inputs) {
	                input.delete();
	            }
	        }
	    }
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void mergeFiles(List<File> listFiles, File out) throws FileNotFoundException, IOException {
		DataWriterFactory writerFactory = RawTextLineWriter.factory();
		merge(listFiles, writerFactory.constructWriter(new FileOutputStream(out)));
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> void mergeStreams(List<InputStream> inputs, DataWriter<T> writer)
	        throws IOException {
	     ArrayList<DataReader<T>> readers = new ArrayList<DataReader<T>>(inputs.size());
	   	 DataReaderFactory readerFactory;
	     Comparator comparator;
	     readerFactory = RawTextLineReader.factory();
         comparator = new ByteArrayComparator();
	        try {
	            for (InputStream mergedInput : inputs) {
	                readers.add(readerFactory.constructReader(mergedInput));
	            }
	            DataReader<T> merger = Merger.mergedReader(comparator, readers);
	            T value;
	            while ((value = merger.readNext()) != null) {
	                writer.writeEntry(value);
	            }
	            writer.close();
	        } finally {
	            for (InputStream input : inputs) {
	                input.close();
	            }
	        }
	    }
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void mergeFiles(List<InputStream> listStreams, FileOutputStream out) throws FileNotFoundException, IOException {
		DataWriterFactory writerFactory = RawTextLineWriter.factory();
		mergeStreams(listStreams, writerFactory.constructWriter((out)));
	}
	    
}
