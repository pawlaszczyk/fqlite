package fqlite.analyzer.bson;

import java.io.FileInputStream;
import java.io.InputStream;
import org.bson.RawBsonDocument;

import fqlite.analyzer.Converter;
import fqlite.base.Job;

public class BSON extends Converter {

	@Override
	public String decode(Job job, String path) {
		String result = "";
		try {
			InputStream is = new FileInputStream(path);
			byte[] bytes = is.readAllBytes();
			RawBsonDocument rbd = new RawBsonDocument(bytes);
			result = rbd.toJson().toString();
			is.close();
		}catch(Exception err) {
			result = "invalid stream";
		}
	
		return result;
	}

}
