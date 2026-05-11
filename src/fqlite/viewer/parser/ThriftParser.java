package fqlite.viewer.parser;

import fqlite.viewer.parser.BinaryNode;
import fqlite.viewer.parser.BinaryNode.Format;
import fqlite.viewer.parser.BinaryNode.Kind;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Schema-less Thrift Binary Protocol and Thrift Compact Protocol decoder.
 *
 * Both decoders start from a struct (field-id + type pairs, terminated by
 * type 0x00). Without a .thrift IDL file we have no field names, so fields
 * are shown as "Field N [TypeName]".
 *
 * ── Binary Protocol ──────────────────────────────────────────────────────
 *   Struct field:  type(1) field_id(2) value
 *   Field types:
 *     0x00 Stop    0x02 Bool    0x03 Byte    0x04 Double
 *     0x06 Int16   0x08 Int32   0x0A Int64   0x0B String/Binary
 *     0x0C Struct  0x0D Map     0x0E Set     0x0F List
 *
 * ── Compact Protocol ─────────────────────────────────────────────────────
 *   Magic byte: 0x82  Protocol id: 0x01  → first byte is always 0x82
 *   Field header: delta(4) | type(4)  or  0x00 type(1) field_id(2)
 *   Compact type ids differ from Binary (bool_true=1, bool_false=2, byte=3,
 *   i16=4, i32=5, i64=6, double=7, binary=8, list=9, set=10, map=11, struct=12)
 */
public class ThriftParser {

    private static final String NAME_BIN     = "Thrift-Binary";
    private static final String NAME_COMPACT = "Thrift-Compact";
    private static final int MAX_DEPTH = 32;

    // ══════════════════════════════════════════════════════════════════════
    // Public API
    // ══════════════════════════════════════════════════════════════════════

    public BinaryNode parseBinary(byte[] data) throws BinaryParseException {
        if (data == null || data.length < 1)
            throw new BinaryParseException(NAME_BIN, "Empty data");
        int[] pos = {0};
        return parseBinaryStruct(data, pos, null, 0, NAME_BIN);
    }

    public BinaryNode parseCompact(byte[] data) throws BinaryParseException {
        if (data == null || data.length < 2)
            throw new BinaryParseException(NAME_COMPACT, "Data too short");
        // Skip the 2-byte message header if present (0x82, version+seq)
        int[] pos = {0};
        if ((data[0] & 0xFF) == 0x82) {
            pos[0] = 0; // keep – the struct starts right here for framed structs
        }
        return parseCompactStruct(data, pos, null, 0);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Thrift Binary
    // ══════════════════════════════════════════════════════════════════════

    private BinaryNode parseBinaryStruct(byte[] data, int[] pos, String fieldName,
                                          int depth, String name) throws BinaryParseException {
        if (depth > MAX_DEPTH)
            throw new BinaryParseException(name, "Nesting depth exceeds " + MAX_DEPTH);

        BinaryNode struct = new BinaryNode(Format.THRIFT_BINARY, Kind.DOCUMENT, fieldName, "Struct");

        while (pos[0] < data.length) {
            int typeCode = data[pos[0]++] & 0xFF;
            if (typeCode == 0x00) break; // STOP field

            if (pos[0] + 2 > data.length)
                throw new BinaryParseException(name, "Truncated field id at " + pos[0]);
            int fieldId = readInt16BE(data, pos);
            String fName = "Field " + fieldId;

            BinaryNode child = parseBinaryValue(data, pos, typeCode, fName, depth, name);
            struct.addChild(child);
        }
        return struct;
    }

    private BinaryNode parseBinaryValue(byte[] data, int[] pos, int typeCode,
                                         String fieldName, int depth, String name)
            throws BinaryParseException {
        return switch (typeCode) {
            case 0x02 -> binScalar(fieldName, "Bool",   data[pos[0]++] != 0 ? "true" : "false");
            case 0x03 -> binScalar(fieldName, "Byte",   String.valueOf((byte)(data[pos[0]++] & 0xFF)));
            case 0x04 -> { // Double
                double d = Double.longBitsToDouble(readInt64BE(data, pos));
                yield binScalar(fieldName, "Double", String.valueOf(d));
            }
            case 0x06 -> binScalar(fieldName, "Int16",  String.valueOf(readInt16BE(data, pos)));
            case 0x08 -> binScalar(fieldName, "Int32",  String.valueOf(readInt32BE(data, pos)));
            case 0x0A -> binScalar(fieldName, "Int64",  String.valueOf(readInt64BE(data, pos)));
            case 0x0B -> { // String / Binary
                int len = readInt32BE(data, pos);
                if (len < 0 || pos[0] + len > data.length)
                    throw new BinaryParseException(name, "Invalid string length " + len);
                byte[] bytes = readExact(data, pos, len);
                // Heuristic: try UTF-8 first
                String s = tryUtf8(bytes);
                if (s != null) yield binScalar(fieldName, "String", "\"" + s + "\"");
                BinaryNode n = new BinaryNode(Format.THRIFT_BINARY, Kind.BINARY, fieldName, "Binary/" + len);
                n.setValue(Base64.getEncoder().encodeToString(bytes) + "  (" + len + " bytes)");
                n.setRawBytes(bytes);
                yield n;
            }
            case 0x0C -> parseBinaryStruct(data, pos, fieldName, depth + 1, name);
            case 0x0D -> { // Map
                int keyType  = data[pos[0]++] & 0xFF;
                int valType  = data[pos[0]++] & 0xFF;
                int count    = readInt32BE(data, pos);
                BinaryNode map = new BinaryNode(Format.THRIFT_BINARY, Kind.MAP, fieldName,
                        "Map<" + binTypeName(keyType) + "," + binTypeName(valType) + ">/" + count);
                for (int i = 0; i < count; i++) {
                    BinaryNode key = parseBinaryValue(data, pos, keyType, null, depth + 1, name);
                    BinaryNode val = parseBinaryValue(data, pos, valType,
                            key.getValue() != null ? key.getValue().replace("\"","") : "[" + i + "]",
                            depth + 1, name);
                    map.addMapEntry(key, val);
                    map.addChild(val);
                }
                yield map;
            }
            case 0x0E, 0x0F -> { // Set / List
                int elemType = data[pos[0]++] & 0xFF;
                int count    = readInt32BE(data, pos);
                String kind  = typeCode == 0x0E ? "Set" : "List";
                BinaryNode arr = new BinaryNode(Format.THRIFT_BINARY, Kind.ARRAY, fieldName,
                        kind + "<" + binTypeName(elemType) + ">/" + count);
                for (int i = 0; i < count; i++)
                    arr.addChild(parseBinaryValue(data, pos, elemType, "[" + i + "]", depth + 1, name));
                yield arr;
            }
            default -> binScalar(fieldName, "Unknown(0x" + Integer.toHexString(typeCode) + ")", "<?>");
        };
    }

    private static String binTypeName(int t) {
        return switch (t) {
            case 0x02 -> "Bool";   case 0x03 -> "Byte";
            case 0x04 -> "Double"; case 0x06 -> "Int16";
            case 0x08 -> "Int32";  case 0x0A -> "Int64";
            case 0x0B -> "String"; case 0x0C -> "Struct";
            case 0x0D -> "Map";    case 0x0E -> "Set";
            case 0x0F -> "List";   default   -> "0x" + Integer.toHexString(t);
        };
    }

    private static BinaryNode binScalar(String fieldName, String typeName, String value) {
        BinaryNode n = new BinaryNode(Format.THRIFT_BINARY, Kind.SCALAR, fieldName, typeName);
        n.setValue(value);
        return n;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Thrift Compact
    // ══════════════════════════════════════════════════════════════════════

    private BinaryNode parseCompactStruct(byte[] data, int[] pos, String fieldName, int depth)
            throws BinaryParseException {
        if (depth > MAX_DEPTH)
            throw new BinaryParseException(NAME_COMPACT, "Nesting depth exceeds " + MAX_DEPTH);

        BinaryNode struct = new BinaryNode(Format.THRIFT_COMPACT, Kind.DOCUMENT, fieldName, "Struct");
        int lastFieldId = 0;

        while (pos[0] < data.length) {
            int header = data[pos[0]++] & 0xFF;
            if (header == 0x00) break; // STOP

            int compactType;
            int fieldId;
            int delta = (header >> 4) & 0x0F;
            compactType = header & 0x0F;

            if (delta == 0) {
                // Long-form: next 2 bytes are zigzag-encoded field id
                fieldId = (int) zigzagDecode(readVarintUnsigned(data, pos));
            } else {
                fieldId = lastFieldId + delta;
            }
            lastFieldId = fieldId;

            String fName = "Field " + fieldId;
            BinaryNode child = parseCompactValue(data, pos, compactType, fName, depth);
            struct.addChild(child);
        }
        return struct;
    }

    private BinaryNode parseCompactValue(byte[] data, int[] pos, int cType,
                                          String fieldName, int depth)
            throws BinaryParseException {
        return switch (cType) {
            case 0x01 -> compScalar(fieldName, "Bool", "true");
            case 0x02 -> compScalar(fieldName, "Bool", "false");
            case 0x03 -> {
                int b = data[pos[0]++] & 0xFF;
                yield compScalar(fieldName, "Byte", String.valueOf((byte) b));
            }
            case 0x04 -> { // i16 – zigzag varint
                long v = readVarintUnsigned(data, pos);
                yield compScalar(fieldName, "Int16", String.valueOf((short) zigzagDecode(v)));
            }
            case 0x05 -> { // i32 – zigzag varint
                long v = readVarintUnsigned(data, pos);
                yield compScalar(fieldName, "Int32", String.valueOf((int) zigzagDecode(v)));
            }
            case 0x06 -> { // i64 – zigzag varint
                long v = readVarintUnsigned(data, pos);
                yield compScalar(fieldName, "Int64", String.valueOf(zigzagDecode(v)));
            }
            case 0x07 -> { // double – 8 bytes little-endian
                double d = Double.longBitsToDouble(readInt64LE(data, pos));
                yield compScalar(fieldName, "Double", String.valueOf(d));
            }
            case 0x08 -> { // binary / string
                int len = (int) readVarintUnsigned(data, pos);
                if (len < 0 || pos[0] + len > data.length)
                    throw new BinaryParseException(NAME_COMPACT, "Invalid binary length " + len);
                byte[] bytes = readExact(data, pos, len);
                String s = tryUtf8(bytes);
                if (s != null) yield compScalar(fieldName, "String", "\"" + s + "\"");
                BinaryNode n = new BinaryNode(Format.THRIFT_COMPACT, Kind.BINARY, fieldName, "Binary/" + len);
                n.setValue(Base64.getEncoder().encodeToString(bytes) + "  (" + len + " bytes)");
                n.setRawBytes(bytes);
                yield n;
            }
            case 0x09 -> { // list
                int sizeType = data[pos[0]++] & 0xFF;
                int elemType = sizeType & 0x0F;
                int count;
                if ((sizeType >> 4) == 0x0F) {
                    count = (int) readVarintUnsigned(data, pos);
                } else {
                    count = (sizeType >> 4) & 0x0F;
                }
                BinaryNode arr = new BinaryNode(Format.THRIFT_COMPACT, Kind.ARRAY, fieldName,
                        "List/" + count);
                for (int i = 0; i < count; i++)
                    arr.addChild(parseCompactValue(data, pos, elemType, "[" + i + "]", depth + 1));
                yield arr;
            }
            case 0x0A -> { // set – same encoding as list
                int sizeType = data[pos[0]++] & 0xFF;
                int elemType = sizeType & 0x0F;
                int count = ((sizeType >> 4) == 0x0F)
                        ? (int) readVarintUnsigned(data, pos) : (sizeType >> 4) & 0x0F;
                BinaryNode arr = new BinaryNode(Format.THRIFT_COMPACT, Kind.ARRAY, fieldName,
                        "Set/" + count);
                for (int i = 0; i < count; i++)
                    arr.addChild(parseCompactValue(data, pos, elemType, "[" + i + "]", depth + 1));
                yield arr;
            }
            case 0x0B -> { // map
                int count = (int) readVarintUnsigned(data, pos);
                BinaryNode map = new BinaryNode(Format.THRIFT_COMPACT, Kind.MAP, fieldName,
                        "Map/" + count);
                if (count > 0) {
                    int kv = data[pos[0]++] & 0xFF;
                    int keyType = (kv >> 4) & 0x0F;
                    int valType = kv & 0x0F;
                    for (int i = 0; i < count; i++) {
                        BinaryNode key = parseCompactValue(data, pos, keyType, null, depth + 1);
                        BinaryNode val = parseCompactValue(data, pos, valType,
                                key.getValue() != null ? key.getValue().replace("\"", "") : "[" + i + "]",
                                depth + 1);
                        map.addMapEntry(key, val);
                        map.addChild(val);
                    }
                }
                yield map;
            }
            case 0x0C -> parseCompactStruct(data, pos, fieldName, depth + 1);
            default   -> compScalar(fieldName, "Unknown(" + cType + ")", "<?>");
        };
    }

    private static BinaryNode compScalar(String fieldName, String typeName, String value) {
        BinaryNode n = new BinaryNode(Format.THRIFT_COMPACT, Kind.SCALAR, fieldName, typeName);
        n.setValue(value);
        return n;
    }

    // ── Low-level helpers ────────────────────────────────────────────────

    private static int readInt16BE(byte[] data, int[] pos) throws BinaryParseException {
        byte[] b = readExact(data, pos, 2);
        return (short)(((b[0] & 0xFF) << 8) | (b[1] & 0xFF));
    }

    private static int readInt32BE(byte[] data, int[] pos) throws BinaryParseException {
        return ByteBuffer.wrap(readExact(data, pos, 4)).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private static long readInt64BE(byte[] data, int[] pos) throws BinaryParseException {
        return ByteBuffer.wrap(readExact(data, pos, 8)).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static long readInt64LE(byte[] data, int[] pos) throws BinaryParseException {
        return ByteBuffer.wrap(readExact(data, pos, 8)).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static long readVarintUnsigned(byte[] data, int[] pos) throws BinaryParseException {
        long value = 0; int shift = 0;
        while (pos[0] < data.length) {
            int b = data[pos[0]++] & 0xFF;
            value |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
            if (shift >= 64) throw new BinaryParseException(NAME_COMPACT, "Varint too long");
        }
        throw new BinaryParseException(NAME_COMPACT, "Truncated varint");
    }

    private static long zigzagDecode(long n) { return (n >>> 1) ^ -(n & 1); }

    private static byte[] readExact(byte[] data, int[] pos, int len) throws BinaryParseException {
        if (pos[0] + len > data.length)
            throw new BinaryParseException("Thrift",
                    "Need " + len + " bytes at offset " + pos[0] + " but only "
                    + (data.length - pos[0]) + " remain");
        byte[] out = new byte[len];
        System.arraycopy(data, pos[0], out, 0, len);
        pos[0] += len;
        return out;
    }

    private static String tryUtf8(byte[] bytes) {
        try {
            java.nio.charset.CharsetDecoder dec =
                    StandardCharsets.UTF_8.newDecoder();
            String s = dec.decode(ByteBuffer.wrap(bytes)).toString();
            long printable = s.chars()
                    .filter(c -> c >= 0x20 || c == '\n' || c == '\r' || c == '\t')
                    .count();
            return (printable >= s.length() * 0.85 && !s.isEmpty()) ? s : null;
        } catch (Exception e) { return null; }
    }
}
