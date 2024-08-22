package fqlite.analyzer.messagepack;

import java.io.FileInputStream;
import java.io.InputStream;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import fqlite.analyzer.Converter;
import fqlite.base.Job;

public class MessagePacker extends Converter {

	@Override
	public String decode(Job job, String path) {
		StringBuffer buffer = new StringBuffer();	
		try {
			InputStream is = new FileInputStream(path);
			MessageUnpacker unpacker = MessagePack.newDefaultUnpacker( is ); 
		  while (unpacker.hasNext()) {
		            
		            // You can also use unpackValue to extract a value of any type
		            Value v = unpacker.unpackValue();
		            buffer.append(v);
			  }
		} catch(Exception err){
			buffer.append("invalid stream");
		}
		return buffer.toString();
	}

}
