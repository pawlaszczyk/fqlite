package fqlite.viewer.parser;

import fqlite.viewer.parser.BinaryNode;
import fqlite.viewer.parser.BinaryNode.Format;
import fqlite.viewer.parser.BinaryNode.Kind;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Schema-less BSON (Binary JSON) parser.
 *
 * BSON spec: https://bsonspec.org/spec.html
 *
 * A BSON document begins with a 4-byte little-endian int32 giving the total
 * document size, followed by a sequence of (type, cstring-key, value) triples,
 * terminated by a 0x00 byte.
 *
 * Supported element types:
 *   0x01 Double       0x02 String      0x03 Document    0x04 Array
 *   0x05 Binary       0x06 Undefined   0x07 ObjectId    0x08 Boolean
 *   0x09 UTCDate      0x0A Null        0x0B Regex        0x0C DBPointer
 *   0x0D JavaScript   0x0E Symbol      0x0F JS w/scope  0x10 Int32
 *   0x11 Timestamp    0x12 Int64       0x13 Decimal128   0xFF MinKey
 *   0x7F MaxKey
 */
public class BsonParser {

    private static final String NAME = "BSON";
    private static final int MAX_DEPTH = 50;

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z")
                             .withZone(ZoneId.of("UTC"));

    // ── Public API ───────────────────────────────────────────────────────

    public BinaryNode parse(byte[] data) throws BinaryParseException {
        if (data == null || data.length < 5)
            throw new BinaryParseException(NAME, "Data too short for a BSON document (min 5 bytes)");
        return parseDocument(data, 0, null, 0);
    }

    // ── Document parser ──────────────────────────────────────────────────

    public BinaryNode parseDocument(byte[] data, int start, String fieldName, int depth)
            throws BinaryParseException {

        if (depth > MAX_DEPTH)
            throw new BinaryParseException(NAME, "Nesting depth exceeds " + MAX_DEPTH);
        if (start + 4 > data.length)
            throw new BinaryParseException(NAME, "Truncated document at offset " + start);

        int docSize = readInt32LE(data, start);
        if (docSize < 5 || start + docSize > data.length)
            throw new BinaryParseException(NAME,
                    "Invalid document size " + docSize + " at offset " + start);

        BinaryNode doc = new BinaryNode(Format.BSON, Kind.DOCUMENT, fieldName, "Document");

        int pos = start + 4;
        int end = start + docSize - 1; // -1 for terminator byte

        while (pos < end) {
            if (pos >= data.length)
                throw new BinaryParseException(NAME, "Unexpected end of data at offset " + pos);

            int typeCode = data[pos++] & 0xFF;
            if (typeCode == 0x00) break; // terminator

            // Read cstring key
            int keyEnd = pos;
            while (keyEnd < data.length && data[keyEnd] != 0) keyEnd++;
            String key = new String(data, pos, keyEnd - pos, StandardCharsets.UTF_8);
            pos = keyEnd + 1; // skip null terminator

            BinaryNode child = parseElement(data, pos, typeCode, key, depth);
            pos = advancePastElement(data, pos, typeCode);
            doc.addChild(child);
        }

        return doc;
    }

    // ── Element parser ───────────────────────────────────────────────────

    public BinaryNode parseElement(byte[] data, int pos, int typeCode, String key, int depth)
            throws BinaryParseException {

        return switch (typeCode) {

            case 0x01 -> { // Double
                double d = Double.longBitsToDouble(readInt64LE(data, pos));
                BinaryNode n = scalar(key, "Float64", String.valueOf(d));
                yield n;
            }
            case 0x02 -> { // UTF-8 String
                int len = readInt32LE(data, pos);
                String s = new String(data, pos + 4, Math.max(0, len - 1), StandardCharsets.UTF_8);
                yield scalar(key, "String", "\"" + s + "\"");
            }
            case 0x03 -> parseDocument(data, pos, key, depth + 1); // Embedded document
            case 0x04 -> { // Array – BSON stores as document with "0","1",… keys
                BinaryNode arrDoc = parseDocument(data, pos, key, depth + 1);
                BinaryNode arr = new BinaryNode(Format.BSON, Kind.ARRAY, key, "Array");
                arrDoc.getChildren().forEach(arr::addChild);
                yield arr;
            }
            case 0x05 -> { // Binary
                int len    = readInt32LE(data, pos);
                int subtype = data[pos + 4] & 0xFF;
                byte[] bytes = readBytes(data, pos + 5, len);
                String subtypeName = binarySubtype(subtype);
                BinaryNode n = new BinaryNode(Format.BSON, Kind.BINARY, key, "Binary/" + subtypeName);
                n.setValue(Base64.getEncoder().encodeToString(bytes)
                        + "  (" + len + " bytes)");
                n.setRawBytes(bytes);
                yield n;
            }
            case 0x06 -> scalar(key, "Undefined", "<undefined>"); // deprecated
            case 0x07 -> { // ObjectId – 12 bytes
                byte[] oid = readBytes(data, pos, 12);
                yield scalar(key, "ObjectId", toHex(oid));
            }
            case 0x08 -> scalar(key, "Boolean", data[pos] != 0 ? "true" : "false");
            case 0x09 -> { // UTC Date
                long ms = readInt64LE(data, pos);
                String ts = DATE_FMT.format(Instant.ofEpochMilli(ms))
                        + "  (ms=" + ms + ")";
                yield scalar(key, "UTCDate", ts);
            }
            case 0x0A -> scalar(key, "Null", "null");
            case 0x0B -> { // Regex – two cstrings: pattern + options
                int p1 = pos;
                while (p1 < data.length && data[p1] != 0) p1++;
                String pattern = new String(data, pos, p1 - pos, StandardCharsets.UTF_8);
                int p2 = p1 + 1;
                while (p2 < data.length && data[p2] != 0) p2++;
                String opts = new String(data, p1 + 1, p2 - p1 - 1, StandardCharsets.UTF_8);
                yield scalar(key, "Regex", "/" + pattern + "/" + opts);
            }
            case 0x0C -> { // DBPointer – string + 12-byte OID
                int len = readInt32LE(data, pos);
                String ns = new String(data, pos + 4, Math.max(0, len - 1), StandardCharsets.UTF_8);
                byte[] oid = readBytes(data, pos + 4 + len, 12);
                yield scalar(key, "DBPointer", ns + " → " + toHex(oid));
            }
            case 0x0D -> { // JavaScript
                int len = readInt32LE(data, pos);
                String js = new String(data, pos + 4, Math.max(0, len - 1), StandardCharsets.UTF_8);
                yield scalar(key, "JavaScript", js);
            }
            case 0x0E -> { // Symbol (deprecated)
                int len = readInt32LE(data, pos);
                String sym = new String(data, pos + 4, Math.max(0, len - 1), StandardCharsets.UTF_8);
                yield scalar(key, "Symbol", sym);
            }
            case 0x0F -> { // JavaScript with scope
                int codeLen = readInt32LE(data, pos + 4);
                String code = new String(data, pos + 8, Math.max(0, codeLen - 1), StandardCharsets.UTF_8);
                yield scalar(key, "JS+Scope", code + "  [+ scope document]");
            }
            case 0x10 -> scalar(key, "Int32",  String.valueOf(readInt32LE(data, pos)));
            case 0x11 -> { // MongoDB Timestamp – two uint32: increment + seconds
                long raw = readInt64LE(data, pos);
                long inc = raw & 0xFFFFFFFFL;
                long sec = (raw >>> 32) & 0xFFFFFFFFL;
                yield scalar(key, "Timestamp", "t=" + sec + "  i=" + inc);
            }
            case 0x12 -> scalar(key, "Int64",  String.valueOf(readInt64LE(data, pos)));
            case 0x13 -> { // Decimal128
                byte[] dec = readBytes(data, pos, 16);
                yield scalar(key, "Decimal128", toHex(dec));
            }
            case 0x7F -> scalar(key, "MaxKey", "<MaxKey>");
            case 0xFF -> scalar(key, "MinKey", "<MinKey>");
            default   -> scalar(key, "Unknown(0x" + Integer.toHexString(typeCode) + ")", "<?>"); 
        };
    }

    /** Returns the byte-length consumed by an element starting at {@code pos}. */
    private int advancePastElement(byte[] data, int pos, int typeCode)
            throws BinaryParseException {
        return pos + switch (typeCode) {
            case 0x01 -> 8;   // double
            case 0x02 -> 4 + readInt32LE(data, pos); // int32 len + bytes incl. null
            case 0x03, 0x04 -> readInt32LE(data, pos); // nested doc
            case 0x05 -> 4 + 1 + readInt32LE(data, pos); // len + subtype + bytes
            case 0x06 -> 0;   // undefined – no value bytes
            case 0x07 -> 12;  // ObjectId
            case 0x08 -> 1;   // bool
            case 0x09 -> 8;   // UTC date
            case 0x0A -> 0;   // null
            case 0x0B -> {    // regex: two cstrings
                int p = pos;
                while (p < data.length && data[p] != 0) p++;
                p++; // skip first null
                while (p < data.length && data[p] != 0) p++;
                p++; // skip second null
                yield p - pos;
            }
            case 0x0C -> 4 + readInt32LE(data, pos) + 12;
            case 0x0D, 0x0E -> 4 + readInt32LE(data, pos);
            case 0x0F -> readInt32LE(data, pos); // total size is first field
            case 0x10 -> 4;
            case 0x11 -> 8;
            case 0x12 -> 8;
            case 0x13 -> 16;
            case 0x7F, 0xFF -> 0;
            default -> throw new BinaryParseException(NAME,
                    "Unknown element type 0x" + Integer.toHexString(typeCode) + " at offset " + pos);
        };
    }

    // ── Low-level helpers ────────────────────────────────────────────────

    private static BinaryNode scalar(String fieldName, String typeName, String value) {
        BinaryNode n = new BinaryNode(Format.BSON, Kind.SCALAR, fieldName, typeName);
        n.setValue(value);
        return n;
    }

    private static int readInt32LE(byte[] data, int offset) {
        return ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static long readInt64LE(byte[] data, int offset) {
        return ByteBuffer.wrap(data, offset, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static byte[] readBytes(byte[] data, int offset, int len) {
        byte[] out = new byte[Math.min(len, data.length - offset)];
        System.arraycopy(data, offset, out, 0, out.length);
        return out;
    }

    public static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02X", x & 0xFF));
        return sb.toString();
    }

    private static String binarySubtype(int sub) {
        return switch (sub) {
            case 0x00 -> "Generic";
            case 0x01 -> "Function";
            case 0x02 -> "BinaryOld";
            case 0x03 -> "UUIDOld";
            case 0x04 -> "UUID";
            case 0x05 -> "MD5";
            case 0x06 -> "EncryptedBSON";
            case 0x07 -> "Column";
            case 0x08 -> "SensitiveField";
            case 0x80 -> "UserDefined";
            default   -> "0x" + Integer.toHexString(sub);
        };
    }
}
