package fqlite.analyzer.thrift;

import java.io.FileInputStream;
import java.io.InputStream;

import org.json.JSONObject;

import fqlite.analyzer.*;
import fqlite.base.Job;


public class ThriftCompactConverter extends Converter {

	@Override
	public String decode(Job job, String path) {
		
		String rvalue = "invalid stream";
		try {

			InputStream is = new FileInputStream(path);
			byte[] bytes = is.readAllBytes();
			is.close();
			
			ThriftCodec tc = new ThriftCodec();
			// call the method for decoding compact protocol stream
			JSONObject obj = tc.decodeCompact(bytes);
			rvalue = obj.toString(4) + "\n";
			System.out.println(rvalue);

		} catch (Exception e) {
			//e.printStackTrace();
		}
		return rvalue;
	}

}
