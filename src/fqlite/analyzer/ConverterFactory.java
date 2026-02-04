package fqlite.analyzer;

import fqlite.analyzer.bson.BSON;
import fqlite.analyzer.fleece.FleeceDecoder;
import fqlite.analyzer.messagepack.MessagePacker;
import fqlite.analyzer.protobuf.ProtobufParser;
import fqlite.analyzer.protobuf.Protoc;
import fqlite.analyzer.thrift.ThriftBinary;
import fqlite.analyzer.thrift.ThriftBinaryConverter;
import fqlite.analyzer.thrift.ThriftCompactConverter;

/**
 * This factory class is used to return the correct decoder class. You need to
 * specify the correct decoder name (see class
 * <code>fqlite.analyzer.Names</code>) to get the correct decoder
 * object. There is exactly one decoder object.
 * 
 */
public class ConverterFactory {

	static Converter protoc = new Protoc();
	static ProtobufParser pbParser = new ProtobufParser();
    static MessagePacker msgpacker = new MessagePacker();
	static BSON bson = new BSON();
	static FleeceDecoder fleece = new FleeceDecoder();
	static ThriftBinary thrift = new ThriftBinary();
	static ThriftCompactConverter thriftcompact = new ThriftCompactConverter();
	static ThriftBinaryConverter thriftbinary = new ThriftBinaryConverter();

	public static Converter build(String bname) {

		Converter product = switch (bname) {
            case Names.BSON -> bson;
            case Names.Fleece -> fleece;
            case Names.MessagePack -> msgpacker;
            case Names.ProtoBuffer -> pbParser;
            case Names.ThriftBinary -> thriftbinary;
            case Names.ThriftCompact -> thriftcompact;
            default -> null;
        };

        return product;
	}

}
