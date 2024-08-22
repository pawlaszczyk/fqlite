package fqlite.analyzer.thrift;

import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TType;

import java.util.HashMap;
import java.util.Map;

final class TypeUtils {

    static final int SIZEOF_I32 = 4;

    private static final Map<Byte, String> messageTypeMap;
    static {
        messageTypeMap = new HashMap<>();
        messageTypeMap.put(TMessageType.CALL, "call");
        messageTypeMap.put(TMessageType.EXCEPTION, "exception");
        messageTypeMap.put(TMessageType.ONEWAY, "oneway");
        messageTypeMap.put(TMessageType.REPLY, "reply");
    }

    private static final Map<Byte, String> typeMap;
    static {
        typeMap = new HashMap<>();
        typeMap.put(TType.STOP, "stop");
        typeMap.put(TType.VOID, "void");
        typeMap.put(TType.BOOL, "bool");
        typeMap.put(TType.BYTE, "byte");
        typeMap.put(TType.DOUBLE, "double");
        typeMap.put(TType.I16, "i16");
        typeMap.put(TType.I32, "i32");
        typeMap.put(TType.I64, "i64");
        typeMap.put(TType.STRING, "string");
        typeMap.put(TType.STRUCT, "struct");
        typeMap.put(TType.MAP, "map");
        typeMap.put(TType.SET, "set");
        typeMap.put(TType.LIST, "list");
        typeMap.put(TType.ENUM, "enum");
    }


    /** Return a string representation of the name of a Thrift type. */
    static String getTypeName (byte type) {
        return typeMap.get(type);
    }

    /** Return integer type code for given type name. */
    static byte getTypeCode (String name) {
        if (name == null) throw new IllegalArgumentException("Name must be non-null");

        for (byte b: typeMap.keySet()) {
            if (typeMap.get(b).equalsIgnoreCase(name)) return b;
        }

        throw new IllegalArgumentException("Unknown type '" + name + "'");
    }


    /** Return a string representation of the name of a Thrift message type. */
    static String getMessageTypeName (byte messageType) {
        return messageTypeMap.get(messageType);
    }

    /** Return integer type code for given message type name. */
    static byte getMessageTypeCode (String name) {
        if (name == null) throw new IllegalArgumentException("Name must be non-null");

        for (byte b: messageTypeMap.keySet()) {
            if (messageTypeMap.get(b).equalsIgnoreCase(name)) return b;
        }

        throw new IllegalArgumentException("Unknown type '" + name + "'");
    }

}