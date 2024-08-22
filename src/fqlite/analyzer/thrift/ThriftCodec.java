package fqlite.analyzer.thrift;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ThriftCodec {

    //private TTransport thriftTransport;
    private TProtocol thriftProtocol;

    public ThriftCodec () {
        // Nothing
    }

    protected TTransport getThriftTransport () {
        return getThriftProtocol().getTransport();
    }

    protected TProtocol getThriftProtocol () {
        return thriftProtocol;
    }

    protected void setThriftProtocol (TProtocol thriftProtocol) {
        this.thriftProtocol = thriftProtocol;
    }

    /* ******************
     * READ/WRITE METHODS
     * ******************/

    /* Single field */

    /**
     * Returns the next field value from the protocol handler based on the given type.
     * @param fieldType TType constant type identifier
     * @return Parsed field value object.
     */
    protected Object readFieldValue (byte fieldType) throws TException {
        TProtocol prot = getThriftProtocol();

        switch(fieldType) {
            case TType.BOOL:
                return prot.readBool();

            case TType.BYTE:
                return prot.readByte();

            case TType.DOUBLE:
                return prot.readDouble();

            case TType.I16:
                return prot.readI16();

            case TType.I32:
                return prot.readI32();

            case TType.I64:
                return prot.readI64();

            case TType.STRING:
                return prot.readString();

            case TType.STRUCT:
                return readStruct();

            case TType.MAP:
                return readMap();

            case TType.SET:
                return readSet();

            case TType.LIST:
                return readList();

            case TType.ENUM:
                throw new UnsupportedOperationException("Enum type reads not supported");

            case TType.VOID:
                throw new UnsupportedOperationException("Void type reads not supported");

            case TType.STOP:
                throw new IllegalArgumentException("Stop type has no value");

            default:
                throw new IllegalArgumentException("Unknown type with value " + (int) fieldType);
        }
    }

    /**
     * Write a field value to the protocol handler.
     * @param fieldType TType constant type ID corresponding to the field type
     * @param value The field value to write
     */
    protected void writeFieldValue (byte fieldType, Object value) throws TException {
        TProtocol prot = getThriftProtocol();

        switch(fieldType) {
            case TType.BOOL:
                if (value instanceof String) {
                    value = Boolean.valueOf((String) value);
                }
                prot.writeBool((boolean) value);
                break;

            case TType.BYTE:
                if (value instanceof String) {
                    String stringRep = (String) value;
                    if (stringRep.length() != 1) {
                        throw new IllegalArgumentException("More than one byte in string representation of byte field");
                    } else {
                        value = stringRep.getBytes()[0];
                    }
                }
                prot.writeByte((byte) value);
                break;

            case TType.DOUBLE:
                if (value instanceof String) {
                    value = Double.parseDouble((String)value); //Double((String) value);
                }
                prot.writeDouble((double) value);
                break;

            case TType.I16:
                if (value instanceof String) {
                    value = Short.parseShort((String) value);
                }
                prot.writeI16((short) value);
                break;

            case TType.I32:
                if (value instanceof String) {
                    value = Integer.parseInt((String) value);
                }
                prot.writeI32((int) value);
                break;

            case TType.I64:
                if (value instanceof String) {
                    value = Long.valueOf((String) value);
                } else if (value instanceof Integer) {
                    value = ((Integer) value).longValue();
                }
                prot.writeI64((long) value);
                break;

            case TType.STRING:
                prot.writeString((String) value);
                break;

            case TType.STRUCT:
                if (value instanceof String) {
                    throw new UnsupportedOperationException("Struct from String not implemented");
                }
                writeStruct((JSONArray) value);
                break;

            case TType.MAP:
                if (value instanceof String) {
                    throw new UnsupportedOperationException("Map from String not implemented");
                }
                writeMap((JSONObject) value);
                break;

            case TType.SET:
                if (value instanceof String) {
                    throw new UnsupportedOperationException("Set from String not implemented");
                }
                writeSet((JSONObject) value);
                break;

            case TType.LIST:
                if (value instanceof String) {
                    throw new UnsupportedOperationException("List from String not implemented");
                }
                writeList((JSONObject) value);
                break;

            case TType.ENUM:
                throw new UnsupportedOperationException("Enum type writes not supported");

            case TType.VOID:
                throw new UnsupportedOperationException("Void type writes not supported");

            case TType.STOP:
                throw new IllegalArgumentException("Stop type has no value");

            default:
                throw new IllegalArgumentException("Unknown type with value " + (int) fieldType);
        }
    }

    /* Field array */

    /**
     * Read an array of fields out of a Thrift protocol handler.
     * @return Returns a JSON array of field objects, which each consist of a field ID, type, and value.
     */
    protected JSONArray readFields () throws TException {
        TProtocol prot = getThriftProtocol();
        JSONArray fields = new JSONArray();

        TField field;
        do {
            field = prot.readFieldBegin();

            JSONObject curField = new JSONObject();
            curField.put("id", field.id);
            curField.put("type", TypeUtils.getTypeName(field.type));

            // Get value based on type
            if (field.type != TType.STOP) {
                curField.put("value", readFieldValue(field.type));
            }

            prot.readFieldEnd();

            if (field.type != TType.STOP) {
                fields.put(curField);
            }
        } while (field.type != TType.STOP);

        return fields;
    }


    /**
     *  Write an array of JSON field values to the Thrift protocol handler.
     * @param fields The JSON array of field values to write.
     */
    protected void writeFields (JSONArray fields) throws TException {
        TProtocol prot = getThriftProtocol();

        for (int i = 0; i < fields.length(); i++) {
            JSONObject fieldJson = fields.getJSONObject(i);

            TField field = new TField("",
                    TypeUtils.getTypeCode(fieldJson.getString("type")),
                    (short) fieldJson.getInt("id"));

            prot.writeFieldBegin(field);

            writeFieldValue(field.type, fieldJson.get("value"));

            prot.writeFieldEnd();
        }

        // Write stop
        prot.writeByte((byte) 0);
    }


    /* Map */

    /**
     * Read out a map from the protocol.
     * @return A JSON object containing the key and value element types, and an object representing the key/value map.
     */
    protected JSONObject readMap () throws TException {
        TProtocol prot = getThriftProtocol();
        TMap map = prot.readMapBegin();

        JSONObject keyVals = new JSONObject();
        for (int i = 0; i < map.size; i++) {
            Object key = readFieldValue(map.keyType);
            Object value = readFieldValue(map.valueType);
            keyVals.put(key.toString(), value);
        }

        prot.readMapEnd();

        JSONObject result = new JSONObject();
        result.put("keyType", TypeUtils.getTypeName(map.keyType));
        result.put("valueType", TypeUtils.getTypeName(map.valueType));
        result.put("map", keyVals);

        return result;
    }

    /**
     * Write a map to the protocol handler.
     * @param mapJson JSON object representing the map to write.
     */
    protected void writeMap (JSONObject mapJson) throws TException {
        TProtocol prot = getThriftProtocol();

        JSONObject keyVals = mapJson.getJSONObject("map");

        TMap map = new TMap(TypeUtils.getTypeCode(mapJson.getString("keyType")),
                TypeUtils.getTypeCode(mapJson.getString("valueType")),
                keyVals.length());

        prot.writeMapBegin(map);

        byte keyType = TypeUtils.getTypeCode(mapJson.getString("keyType"));
        byte valueType = TypeUtils.getTypeCode(mapJson.getString("valueType"));

        for (String key: keyVals.keySet()) {
            writeFieldValue(keyType, key);
            writeFieldValue(valueType, keyVals.get(key));
        }

        prot.writeMapEnd();
    }


    /* List */

    /**
     * Read out a list from the protocol.
     * @return A JSON object containing the list's element type and an array of the list's elements.
     */
    protected JSONObject readList () throws TException {
        TProtocol prot = getThriftProtocol();

        TList list = prot.readListBegin();

        JSONArray listJson = new JSONArray();
        for (int i = 0; i < list.size; i++) {
            listJson.put(readFieldValue(list.elemType));
        }

        prot.readListEnd();

        JSONObject result = new JSONObject();
        result.put("elemType", TypeUtils.getTypeName(list.elemType));
        result.put("list", listJson);

        return result;
    }

    /**
     * Write a list to the protocol.
     * @param listJson A JSON object containing the list's element type and an array of the list's elements.
     */
    protected void writeList (JSONObject listJson) throws TException {
        TProtocol prot = getThriftProtocol();

        byte elemType = TypeUtils.getTypeCode(listJson.getString("elemType"));
        JSONArray actualList = listJson.getJSONArray("list");

        TList list = new TList(elemType, actualList.length());

        prot.writeListBegin(list);

        for (int i = 0; i < list.size; i++) {
            writeFieldValue(list.elemType, actualList.get(i));
        }

        prot.writeListEnd();
    }


    /* Set */

    /**
     * Read out a set from the protocol
     * @return A JSON object containing the set's element type and an array of the set's elements.
     */
    protected JSONObject readSet () throws TException {
        TProtocol prot = getThriftProtocol();

        TSet set = prot.readSetBegin();

        JSONArray setJson = new JSONArray();
        for (int i = 0; i < set.size; i++) {
            setJson.put(readFieldValue(set.elemType));
        }

        prot.readSetEnd();

        JSONObject result = new JSONObject();
        result.put("elemType", TypeUtils.getTypeName(set.elemType));
        result.put("set", setJson);

        return result;
    }

    /**
     * Write a set to the protocol
     * @param setJson A JSON object containing the set's element type and an array of the set's elements.
     */
    protected void writeSet (JSONObject setJson) throws TException {
        TProtocol prot = getThriftProtocol();

        byte elemType = TypeUtils.getTypeCode(setJson.getString("elemType"));
        JSONArray actualSet = setJson.getJSONArray("set");

        TSet set = new TSet(elemType, actualSet.length());

        prot.writeSetBegin(set);

        for (int i = 0; i < set.size; i++) {
            writeFieldValue(set.elemType,  actualSet.get(i));
        }

        prot.writeSetEnd();
    }

    /* Struct */

    protected JSONArray readStruct () throws TException {
        TProtocol prot = getThriftProtocol();

        prot.readStructBegin();
        JSONArray structJson = readFields();
        prot.readStructEnd();

        return structJson;
    }

    protected void writeStruct (JSONArray structJson) throws TException {
        TProtocol prot = getThriftProtocol();

        prot.writeStructBegin(new TStruct());
        writeFields(structJson);
        prot.writeStructEnd();
    }


    /* Thrift Message */

    protected JSONObject readMessage () throws TException {
        TProtocol prot = getThriftProtocol();

        JSONObject messageJson = new JSONObject();

       // TMessage msg = prot.readMessageBegin();

       // messageJson.put("name", msg.name);
       // messageJson.put("seqid", msg.seqid);
       // messageJson.put("type", TypeUtils.getMessageTypeName(msg.type));

        // Read fields
        messageJson.put("fields", readFields());

        prot.readMessageEnd();

        return messageJson;
    }

    protected void writeMessage (JSONObject msgJson) throws TException {
        TProtocol prot = getThriftProtocol();

        TMessage msg = new TMessage(msgJson.getString("name"),
                TypeUtils.getMessageTypeCode(msgJson.getString("type")),
                msgJson.getInt("seqid"));

        prot.writeMessageBegin(msg);

        writeFields(msgJson.getJSONArray("fields"));

        prot.writeMessageEnd();
    }


    /* *************
     * CODEC METHODS
     * *************/

    public JSONObject decode (byte[] binaryBuffer) throws TException, JSONException {
        // Set up buffer for feeding into Thrift protocol
        TTransport byteBuffer = new TMemoryBuffer(binaryBuffer.length);
        setThriftProtocol(new TBinaryProtocol(byteBuffer));
        byteBuffer.write(binaryBuffer);

        // Read the Thrift message
        JSONObject messageJson = readMessage();

        // Build the JSON result
        JSONObject result = new JSONObject();
        result.put("message", messageJson);

        return result;
    }

    public JSONObject decodeCompact (byte[] binaryBuffer) throws TException, JSONException {
        // Set up buffer for feeding into Thrift protocol
        TTransport byteBuffer = new TMemoryBuffer(binaryBuffer.length);
        setThriftProtocol(new TCompactProtocol(byteBuffer));
        byteBuffer.write(binaryBuffer);

        // Read the Thrift message
        JSONObject messageJson = readMessage();

        // Build the JSON result
        JSONObject result = new JSONObject();
        result.put("message", messageJson);

        return result;
    }
    
    
    /**
     * Encode a Thrift JSON object into a binary protocol buffer.
     */
    public byte[] encode (JSONObject thriftJson) throws TException, JSONException {
        AutoExpandingBufferWriteTransport transportBuffer = new AutoExpandingBufferWriteTransport(32000, 1.5);
        setThriftProtocol(new TBinaryProtocol(transportBuffer));

        // Write the Thrift message
        writeMessage(thriftJson.getJSONObject("message"));

        // Flush and return the buffer
        transportBuffer.flush();
        transportBuffer.close();
        byte[] finalBuf = transportBuffer.getBuf().array();

        int resultSize = transportBuffer.getPos();

        // Put the output buffer here
        byte[] result = new byte[resultSize];
        System.arraycopy(finalBuf, 0, result, 0, resultSize);

        return result;
    }

    /**
     * Pass Base64 encoded Thrift binary protocol message through to decoder.
     */
    public static JSONObject decodeB64String (String input) throws TException, JSONException {
        // Base64 decode the input string
        byte[] binaryBuffer = Base64.decodeBase64(input);
        ThriftCodec codec = new ThriftCodec();
        return codec.decode(binaryBuffer);
    }


    /**
     * Return Base64 representation of binary protocol Thrift message for given Thrift JSON object.
     */
    public static String b64encodeJson (JSONObject thriftJson) throws TException, JSONException {
        ThriftCodec codec = new ThriftCodec();
        return Base64.encodeBase64String(codec.encode(thriftJson));
    }

}