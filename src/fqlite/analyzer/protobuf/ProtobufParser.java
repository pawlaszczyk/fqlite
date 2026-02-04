package fqlite.analyzer.protobuf;

import fqlite.analyzer.Converter;
import fqlite.base.Job;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ProtobufParser extends Converter {

    public static void main(String[] args) {
        // Example: Parse a protobuf byte array
        // Field 1 (varint): 150, Field 2 (string): "testing"
        byte[] protobufData = {0x08, -106, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67};

        System.out.println("Parsing protobuf data...\n");
        Map<Integer, List<Object>> parsed = parseProtobuf(protobufData,true);
        String result = prepareOutput(parsed);
        System.out.println(result);
    }

    public static Map<Integer, List<Object>> parseProtobuf(byte[] data) {
        return parseProtobuf(data, false);
    }

    private static Map<Integer, List<Object>> parseProtobuf(byte[] data, boolean silent) {
        Map<Integer, List<Object>> fields = new LinkedHashMap<>();
        int pos = 0;

        while (pos < data.length) {
            try {
                // Read field tag
                VarIntResult tagResult = readVarInt(data, pos);
                long tag = tagResult.value;
                pos = tagResult.nextPos;

                int fieldNumber = (int)(tag >>> 3);
                int wireType = (int)(tag & 0x7);

                fields.putIfAbsent(fieldNumber, new ArrayList<>());

                /*
                There are six wire types: VARINT, I64, LEN, SGROUP, EGROUP, and I32
                    ID	Name	Used For
                    0	VARINT	int32, int64, uint32, uint64, sint32, sint64, bool, enum
                    1	I64	fixed64, sfixed64, double
                    2	LEN	string, bytes, embedded messages, packed repeated fields
                    3	SGROUP	group start (deprecated)
                    4	EGROUP	group end (deprecated)
                    5	I32	fixed32, sfixed32, float
                */

                switch (wireType) {
                    case 0: // Varint
                        VarIntResult varintResult = readVarInt(data, pos);
                        pos = varintResult.nextPos;
                        fields.get(fieldNumber).add(interpretVarint(varintResult.value));
                        break;

                    case 1: // 64-bit
                        if (pos + 8 > data.length) throw new RuntimeException("Incomplete 64-bit field");
                        long fixed64 = readFixed64(data, pos);
                        pos += 8;
                        fields.get(fieldNumber).add(interpret64Bit(fixed64));
                        break;

                    case 2: // Length-delimited
                        VarIntResult lenResult = readVarInt(data, pos);
                        int length = (int)lenResult.value;
                        pos = lenResult.nextPos;

                        if (pos + length > data.length) throw new RuntimeException("Incomplete length-delimited field");
                        byte[] bytes = Arrays.copyOfRange(data, pos, pos + length);
                        pos += length;
                        fields.get(fieldNumber).add(interpretLengthDelimited(bytes));
                        break;

                    case 5: // 32-bit
                        if (pos + 4 > data.length) throw new RuntimeException("Incomplete 32-bit field");
                        int fixed32 = readFixed32(data, pos);
                        pos += 4;
                        fields.get(fieldNumber).add(interpret32Bit(fixed32));
                        break;

                    default:
                        throw new RuntimeException("Unknown wire type: " + wireType);
                }
            } catch (Exception e) {
                if (!silent) {
                    System.err.println("Error parsing at position " + pos + ": " + e.getMessage());
                }
                break;
            }
        }

        return fields;
    }

    private static VarIntResult readVarInt(byte[] data, int pos) {
        long result = 0;
        int shift = 0;

        while (pos < data.length) {
            byte b = data[pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return new VarIntResult(result, pos);
            }
            shift += 7;
            if (shift >= 64)  throw new RuntimeException("Varint too long");
        }
        throw new RuntimeException("Incomplete varint");
    }

    private static long readFixed64(byte[] data, int pos) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result |= ((long)(data[pos + i] & 0xFF)) << (i * 8);
        }
        return result;
    }

    private static int readFixed32(byte[] data, int pos) {
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result |= ((data[pos + i] & 0xFF)) << (i * 8);
        }
        return result;
    }

    private static Object interpretVarint(long value) {
        // Try to guess the best representation
        Map<String, Object> interpretations = new LinkedHashMap<>();

        interpretations.put("uint64", value);
        interpretations.put("int64", value);

        // ZigZag decoding for sint64
        long zigzag = (value >>> 1) ^ -(value & 1);
        interpretations.put("sint64", zigzag);

        interpretations.put("bool", value != 0);

        return interpretations;
    }

    private static Object interpret64Bit(long value) {
        Map<String, Object> interpretations = new LinkedHashMap<>();
        interpretations.put("fixed64", value);
        interpretations.put("sfixed64", value);
        interpretations.put("double", Double.longBitsToDouble(value));
        return interpretations;
    }

    private static Object interpret32Bit(int value) {
        Map<String, Object> interpretations = new LinkedHashMap<>();
        interpretations.put("fixed32", value & 0xFFFFFFFFL);
        interpretations.put("sfixed32", value);
        interpretations.put("float", Float.intBitsToFloat(value));
        return interpretations;
    }

    private static Object interpretLengthDelimited(byte[] bytes) {
        Map<String, Object> interpretations = new LinkedHashMap<>();

        // Try as UTF-8 string
        try {
            String str = new String(bytes, StandardCharsets.UTF_8);
            if (isPrintable(str)) {
                interpretations.put("string", str);
            }
        } catch (Exception ignored) {}

        // Try as nested message (silently - don't print errors for failed attempts)
        try {
            Map<Integer, List<Object>> nested = parseProtobuf(bytes, true);
            if (!nested.isEmpty()) {
                interpretations.put("message", nested);
            }
        } catch (Exception ignored) {}

        // Raw bytes
        interpretations.put("bytes", bytesToHex(bytes));

        return interpretations;
    }

    private static boolean isPrintable(String str) {
        for (char c : str.toCharArray()) {
            if (c < 32 && c != '\n' && c != '\r' && c != '\t') return false;
            if (c == 127) return false;
        }
        return true;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    private static String prepareOutput(Map<Integer, List<Object>> fields) {

        StringBuilder sb = new StringBuilder();

        sb.append("\nParsed Protobuf Data:" + "\n");
        sb.append("====================");

        for (Map.Entry<Integer, List<Object>> entry : fields.entrySet()) {
            sb.append("\nField " + entry.getKey() + ":" + "\n");
            for (Object value : entry.getValue()) {
                printValue(value, 1, sb);
            }
        }

        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static String printValue(Object value, int indent, StringBuilder sb) {
        String indentStr = "  ".repeat(indent);

        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map.isEmpty()) return sb.toString();

            // Check if it's an interpretations or a nested message
            Object firstValue = map.values().iterator().next();
            if (firstValue instanceof List) {
                // Nested message
                sb.append(indentStr + "Nested Message:" + "\n");
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    sb.append(indentStr + "  Field " + e.getKey() + ":" + "\n");
                    for (Object v : (List<?>)e.getValue()) {
                        sb.append(printValue(v, indent + 2, sb) + "\n");
                    }
                }
            } else {
                // Interpretations
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    sb.append(indentStr + e.getKey() + ": " + e.getValue() + "\n");
                }
            }
        } else {
            sb.append(indentStr + value);
        }
        return sb.toString();
    }

    @Override
    public String decode(Job job, String path) {

        byte[] protobufData = null;
        try {
            protobufData = Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        System.out.println("Parsing protobuf data...\n");
        Map<Integer, List<Object>> parsed = parseProtobuf(protobufData,true);

        return prepareOutput(parsed);
    }

    private static class VarIntResult {
        long value;
        int nextPos;

        VarIntResult(long value, int nextPos) {
            this.value = value;
            this.nextPos = nextPos;
        }
    }
}