package fqlite.analyzer.avro;

import java.io.File;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class Avro{

	public static String decode(String path) {

		StringBuffer buffer = new StringBuffer();
		try {
			File file = new File(path);
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
			GenericRecord user = null;
			while (dataFileReader.hasNext()) {
				user = dataFileReader.next(user);
				buffer.append(user);
			}
			dataFileReader.close();
		}
		catch(Exception err){
			return "invalid stream";
		}
		
			
		return buffer.toString();
	}

}
