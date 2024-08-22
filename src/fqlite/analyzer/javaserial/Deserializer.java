package fqlite.analyzer.javaserial;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import nb.deser.SerializationDumper;

public class Deserializer {
	
	
	public static String decode(String filename){
		
	    
		
		String [] arg = {"-r",filename};
		// Create a stream to hold the output
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		
		try {
			
			
			// IMPORTANT: Save the old System.out!
			PrintStream old = System.out;
			// Tell Java to use your special stream
			System.setOut(ps);
			
			// start decoding the stream file
			SerializationDumper.main(arg);
			
			// Put things back
			System.out.flush();
			System.setOut(old);
			// Show what happened
			//System.out.println(baos.toString());
			
			
		} catch (Exception e) {
			// Do nothing
		    return "deserialization error";
		}
		
		return baos.toString();
	}
	
	
}
