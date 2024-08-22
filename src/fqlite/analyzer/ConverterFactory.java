package fqlite.analyzer;

import fqlite.analyzer.bson.BSON;
import fqlite.analyzer.fleece.FleeceDecoder;
import fqlite.analyzer.messagepack.MessagePacker;
import fqlite.analyzer.protobuf.Protoc;
import fqlite.analyzer.thrift.ThriftBinary;
import fqlite.analyzer.thrift.ThriftBinaryConverter;
import fqlite.analyzer.thrift.ThriftCompactConverter;

/**
 * This factory class is used to return the correct decoder class. You need to
 * specify the correct decoder name (see class
 * <code>fqlite.analyzer.Names</code>) in order to get the correct decoder
 * object. There is exactly one decoder object.
 * 
 */
public class ConverterFactory {

	static Converter protoc = new Protoc();
	static MessagePacker msgpacker = new MessagePacker();
	static BSON bson = new BSON();
	static FleeceDecoder fleece = new FleeceDecoder();
	static ThriftBinary thrift = new ThriftBinary();
	static ThriftCompactConverter thriftcompact = new ThriftCompactConverter();
	static ThriftBinaryConverter thriftbinary = new ThriftBinaryConverter();

	public static Converter build(String bname) {

		Converter product = null;

		switch (bname) {

		case Names.BSON: product = bson;
			break;
		case Names.Fleece: product = fleece;
			break;
		case Names.MessagePack: product = msgpacker;
			break;
		case Names.ProtoBuffer: product = protoc;
			break;
		case Names.ThriftBinary: product = thriftbinary;
			break;
		case Names.ThriftCompact: product = thriftcompact;
			break;
		}

		return product;
	}

}
